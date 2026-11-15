package com.payflow.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.payflow.ledger.anomaly.AnomalyDetector;
import com.payflow.ledger.anomaly.AnomalyResult;
import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import com.payflow.ledger.web.ChainVerificationResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 6: proves, against a REAL Postgres, that concurrent {@code record()} calls can
 * never commit a duplicate {@code chain_index}. {@code @DataJpaTest} normally wraps each
 * test in a transaction; that is turned off (NOT_SUPPORTED) because the writers must be
 * independent transactions, exactly as in production.
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class LedgerChainConcurrencyTest {

    private static final java.util.TimeZone ORIGINAL_TZ = java.util.TimeZone.getDefault();

    static {
        // The pgjdbc driver sends the JVM zone to Postgres; legacy ids such as "Asia/Calcutta"
        // (the Windows default for India) are rejected by the postgres image. Must run before
        // the first connection, hence a static initialiser; restored in @AfterAll.
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
    }

    @org.junit.jupiter.api.AfterAll
    static void restoreTimeZone() {
        java.util.TimeZone.setDefault(ORIGINAL_TZ);
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        // schema comes from the real Flyway V1 baseline, validated by Hibernate (ddl-auto: validate)
    }

    @Autowired
    LedgerRepository repository;

    private MeterRegistry meters;
    private LedgerService service;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        meters = new SimpleMeterRegistry();
        AnomalyDetector detector = mock(AnomalyDetector.class);
        when(detector.detect(any(LedgerEntry.class))).thenReturn(AnomalyResult.normal());
        service = new LedgerService(repository, meters, detector);
    }

    private static LedgerEntry entry() {
        LedgerEntry e = new LedgerEntry();
        e.setTransactionId(UUID.randomUUID());
        e.setFromAccount(UUID.randomUUID());
        e.setToAccount(UUID.randomUUID());
        e.setAmount(new BigDecimal("10.00"));
        e.setCurrency("INR");
        return e;
    }

    @Test
    void databaseRejectsTwoEntriesWithTheSameChainIndex() {
        LedgerEntry a = entry();
        a.setChainIndex(0L);
        a.setPreviousHash("0".repeat(64));
        a.setEntryHash("a".repeat(64));
        a.setRecordedAt(java.time.Instant.now());
        repository.saveAndFlush(a);

        LedgerEntry b = entry();
        b.setChainIndex(0L);
        b.setPreviousHash("0".repeat(64));
        b.setEntryHash("b".repeat(64));
        b.setRecordedAt(java.time.Instant.now());

        assertThatThrownBy(() -> repository.saveAndFlush(b))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void twoNearSimultaneousRecordCallsNeverShareAChainIndex() throws Exception {
        int rounds = 25;
        for (int round = 0; round < rounds; round++) {
            runConcurrently(2, 1);
        }
        assertChainIsSoundWith(rounds * 2);
        System.out.println("[Phase6] 2 writers x " + rounds + " rounds, chain-index retries: "
                + meters.counter("ledger.chain.retries").count());
    }

    @Test
    void burstsOfFourSimultaneousWritersProduceAContiguousUniqueChain() throws Exception {
        // A burst of N simultaneous writers can lose at most N-1 times (every collision means
        // another writer in the burst committed), so the 5-attempt cap always suffices here.
        // Sustained back-to-back writing by several threads is NOT guaranteed by retry+cap
        // (a fast writer can starve a slower one); see the Phase 6 Agent Log entry.
        int rounds = 10;
        for (int round = 0; round < rounds; round++) {
            runConcurrently(4, 1);
        }
        assertChainIsSoundWith(rounds * 4);
        System.out.println("[Phase6] 4-writer bursts x " + rounds + ", chain-index retries: "
                + meters.counter("ledger.chain.retries").count());
    }

    /** {@code writers} threads released together by a barrier, each recording {@code each} entries. */
    private void runConcurrently(int writers, int each) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        CyclicBarrier barrier = new CyclicBarrier(writers);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            futures.add(pool.submit(() -> {
                barrier.await();
                for (int i = 0; i < each; i++) {
                    service.record(entry());
                }
                return null;
            }));
        }
        try {
            for (Future<?> f : futures) {
                f.get(); // rethrows any record() failure, so an exhausted retry cap fails the test
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private void assertChainIsSoundWith(int expectedEntries) {
        List<LedgerEntry> chain = repository.findAllByOrderByChainIndexAsc();
        assertThat(chain).hasSize(expectedEntries);
        List<Long> indexes = chain.stream().map(LedgerEntry::getChainIndex).collect(Collectors.toList());
        assertThat(indexes).doesNotHaveDuplicates();
        // contiguous 0..n-1: no gaps, nothing skipped
        for (int i = 0; i < indexes.size(); i++) {
            assertThat(indexes.get(i)).isEqualTo((long) i);
        }
        ChainVerificationResult v = service.verifyChain();
        assertThat(v.valid()).as(v.message()).isTrue();
    }
}
