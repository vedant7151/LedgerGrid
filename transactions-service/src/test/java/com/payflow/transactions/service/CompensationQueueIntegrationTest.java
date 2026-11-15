package com.payflow.transactions.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.payflow.transactions.domain.CompensationEntry;
import com.payflow.transactions.domain.CompensationStatus;
import com.payflow.transactions.repository.CompensationEntryRepository;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 7 acceptance: force a compensation failure, see it land in the queue, see it
 * self-heal. Real pieces: Postgres (Testcontainers), the full Spring context with the
 * real scheduler, and the real AccountsClient over real HTTP. The only fake is the
 * Accounts service, a tiny JDK HttpServer whose failures this test controls, which
 * is how "accounts-service goes down during the refund step" is forced deterministically.
 */
@Testcontainers
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = {
        "compensation.base-delay-ms=300",
        "compensation.poll-interval-ms=100",
        "compensation.max-attempts=4",
        "outbox.poll-interval-ms=3600000",           // keep the outbox poller quiet; no Kafka here
        "spring.kafka.bootstrap-servers=localhost:1", // never contacted
        "spring.kafka.admin.fail-fast=false"
        // schema comes from the real Flyway V1 baseline, validated by Hibernate (ddl-auto: validate)
})
class CompensationQueueIntegrationTest {

    private static final java.util.TimeZone ORIGINAL_TZ = java.util.TimeZone.getDefault();

    static {
        // pgjdbc sends the JVM zone; Windows' legacy "Asia/Calcutta" is rejected by Postgres.
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
    }

    @AfterAll
    static void tearDown() {
        java.util.TimeZone.setDefault(ORIGINAL_TZ);
        fakeAccounts.stop(0);
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    // ---- controllable fake accounts-service --------------------------------------------
    static final UUID FROM = UUID.randomUUID();
    static final UUID TO = UUID.randomUUID();
    /** How many refund (credit to FROM) calls must still fail before accounts "comes back". */
    static final AtomicInteger refundFailuresLeft = new AtomicInteger();
    static final AtomicInteger refundCalls = new AtomicInteger();
    static final AtomicInteger refundSuccesses = new AtomicInteger();
    static HttpServer fakeAccounts;

    static {
        try {
            fakeAccounts = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        fakeAccounts.createContext("/accounts/", ex -> {
            String path = ex.getRequestURI().getPath();
            if (path.endsWith("/debit")) {
                respond(ex, 200, account(FROM));
            } else if (path.contains(TO.toString())) {
                respond(ex, 500, "{\"error\":\"receiver unavailable\"}");   // the credit that triggers compensation
            } else if (path.contains(FROM.toString())) {                      // the compensating refund
                refundCalls.incrementAndGet();
                if (refundFailuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                    respond(ex, 503, "{\"error\":\"accounts down\"}");
                } else {
                    refundSuccesses.incrementAndGet();
                    respond(ex, 200, account(FROM));
                }
            } else {
                respond(ex, 404, "{}");
            }
        });
        fakeAccounts.start();
    }

    private static String account(UUID id) {
        return "{\"id\":\"" + id + "\",\"ownerName\":\"A\",\"balance\":1000,\"currency\":\"INR\","
                + "\"createdAt\":\"2026-01-01T00:00:00Z\"}";
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        ex.getRequestBody().readAllBytes();
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("accounts.base-url", () -> "http://127.0.0.1:" + fakeAccounts.getAddress().getPort());
    }

    @Autowired
    TransactionService transactionService;
    @Autowired
    CompensationEntryRepository queue;

    @BeforeEach
    void reset() {
        queue.deleteAll();
        refundCalls.set(0);
        refundSuccesses.set(0);
    }

    @Test
    void failedRefundLandsInTheQueueThenSelfHealsWhenAccountsComesBack() {
        refundFailuresLeft.set(2); // inline refund fails, and so does the 1st retry; the 2nd retry succeeds

        assertThatThrownBy(() -> transactionService.transfer(
                FROM, TO, new BigDecimal("50.00"), "forced failure", "X", null))
                .isInstanceOf(TransferFailedException.class);

        // 1. It landed in the queue (the inline refund was attempted once and failed).
        List<CompensationEntry> rows = queue.findAll();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getReason()).contains("503");
        assertThat(rows.get(0).getStatus()).isIn(CompensationStatus.PENDING, CompensationStatus.RESOLVED);
        assertThat(refundCalls.get()).isGreaterThanOrEqualTo(1);

        // 2. It self-heals once the retry job's refund finally succeeds.
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            CompensationEntry e = queue.findAll().get(0);
            assertThat(e.getStatus()).isEqualTo(CompensationStatus.RESOLVED);
        });
        CompensationEntry resolved = queue.findAll().get(0);
        assertThat(resolved.getAttemptCount()).isEqualTo(2);       // exactly 2 retries: one failed, one succeeded
        assertThat(refundSuccesses.get()).isEqualTo(1);             // the sender was refunded exactly once
        assertThat(refundCalls.get()).isEqualTo(3);                 // inline + 2 retries, no extra calls after resolving
    }

    @Test
    void givesUpAndMarksFailedPermanentlyWhenAccountsNeverRecovers() {
        refundFailuresLeft.set(Integer.MAX_VALUE);

        assertThatThrownBy(() -> transactionService.transfer(
                FROM, TO, new BigDecimal("20.00"), "never recovers", "X", null))
                .isInstanceOf(TransferFailedException.class);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() ->
                assertThat(queue.findAll().get(0).getStatus()).isEqualTo(CompensationStatus.FAILED_PERMANENTLY));
        CompensationEntry e = queue.findAll().get(0);
        assertThat(e.getAttemptCount()).isEqualTo(4);               // compensation.max-attempts
        assertThat(refundSuccesses.get()).isZero();
        int callsAtGiveUp = refundCalls.get();
        assertThat(callsAtGiveUp).isEqualTo(5);                     // inline + 4 retries
        // and it really stopped: no further refund attempts after the cap
        try {
            Thread.sleep(1500);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        assertThat(refundCalls.get()).isEqualTo(callsAtGiveUp);
    }
}
