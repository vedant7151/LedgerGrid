package com.payflow.ledger.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.payflow.ledger.repository.LedgerRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Phase 8 acceptance: a malformed message on {@code transactions} must land on
 * {@code transactions.DLT} and must NOT block the consumer. Real Kafka, Postgres and
 * Redis (Testcontainers) with the full application context and the real @KafkaListener.
 */
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = {
        "ledger.dlt.initial-interval-ms=100",   // fast retries for the test (production default 1s, 2s, 4s)
        "ledger.dlt.max-retries=2"
        // schema comes from the real Flyway V1 baseline, validated by Hibernate (ddl-auto: validate)
})
class DeadLetterTopicIntegrationTest {

    private static final java.util.TimeZone ORIGINAL_TZ = java.util.TimeZone.getDefault();

    static {
        // pgjdbc sends the JVM zone; Windows' legacy "Asia/Calcutta" is rejected by Postgres.
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
    }

    @AfterAll
    static void restoreTimeZone() {
        java.util.TimeZone.setDefault(ORIGINAL_TZ);
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7").withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    LedgerRepository ledgerRepository;

    private static String validEvent(UUID txnId) {
        return "{\"transactionId\":\"" + txnId + "\",\"fromAccount\":\"" + UUID.randomUUID()
                + "\",\"toAccount\":\"" + UUID.randomUUID() + "\",\"amount\":25.00,\"currency\":\"INR\","
                + "\"description\":\"dlt test\",\"merchant\":\"TestMart\",\"timestamp\":\"2026-01-01T00:00:00Z\"}";
    }

    private static void send(String key, String value) throws Exception {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            p.send(new ProducerRecord<>("transactions", key, value)).get();
        }
    }

    /** Reads everything currently on the DLT from the start. */
    private static List<ConsumerRecord<String, byte[]>> readDlt() {
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            c.subscribe(List.of("transactions.DLT"));
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            c.poll(Duration.ofSeconds(2)).forEach(out::add);
            return out;
        }
    }

    private static boolean hasHeader(ConsumerRecord<?, ?> rec, String name) {
        for (Header h : rec.headers()) {
            if (h.key().equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Test
    void malformedMessageLandsOnTheDltAndDoesNotBlockTheConsumer() throws Exception {
        String malformed = "this is {not valid json";
        UUID goodTxn = UUID.randomUUID();
        long entriesBefore = ledgerRepository.count(); // tests share one DB, so compare against a baseline

        send("bad-key", malformed);          // poison pill first ...
        send(goodTxn.toString(), validEvent(goodTxn)); // ... a good event right behind it

        // 1. The consumer was not blocked: the good event behind the bad one is recorded.
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(ledgerRepository.existsByTransactionId(goodTxn)).isTrue());

        // 2. The malformed message is on the DLT with its original bytes and failure headers.
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            List<ConsumerRecord<String, byte[]>> dlt = readDlt();
            ConsumerRecord<String, byte[]> rec = dlt.stream()
                    .filter(r -> r.value() != null
                            && new String(r.value(), StandardCharsets.UTF_8).equals(malformed))
                    .findFirst().orElse(null);
            assertThat(rec).as("malformed record on transactions.DLT (DLT size=" + dlt.size() + ")").isNotNull();
            assertThat(hasHeader(rec, "kafka_dlt-exception-fqcn")).isTrue();
            assertThat(hasHeader(rec, "kafka_dlt-original-topic")).isTrue();
        });
        // only the good event was recorded; the poison record created no ledger entry
        assertThat(ledgerRepository.count())
                .as("before=" + entriesBefore + " goodTxn=" + goodTxn + " entries="
                        + ledgerRepository.findAll().stream()
                                .map(e -> e.getTransactionId() + "/" + e.getDescription()).toList())
                .isEqualTo(entriesBefore + 1);
    }

    @Test
    void recordThatFailsProcessingIsRetriedThenDeadLettered() throws Exception {
        // Valid JSON, but processing throws (amount is NOT NULL in the ledger): this exercises the
        // retry-with-backoff path and the JSON-serialised DLT payload, as opposed to the
        // deserialization-failure path above.
        UUID poisonTxn = UUID.randomUUID();
        String poison = "{\"transactionId\":\"" + poisonTxn + "\",\"fromAccount\":\"" + UUID.randomUUID()
                + "\",\"toAccount\":\"" + UUID.randomUUID() + "\",\"amount\":null,\"currency\":\"INR\"}";
        UUID goodTxn = UUID.randomUUID();

        send(poisonTxn.toString(), poison);
        send(goodTxn.toString(), validEvent(goodTxn));

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(ledgerRepository.existsByTransactionId(goodTxn)).isTrue());

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            List<ConsumerRecord<String, byte[]>> dlt = readDlt();
            ConsumerRecord<String, byte[]> rec = dlt.stream()
                    .filter(r -> r.value() != null
                            && new String(r.value(), StandardCharsets.UTF_8).contains(poisonTxn.toString()))
                    .findFirst().orElse(null);
            assertThat(rec).as("failed record on transactions.DLT").isNotNull();
            assertThat(hasHeader(rec, "kafka_dlt-exception-fqcn")).isTrue();
        });
        assertThat(ledgerRepository.existsByTransactionId(poisonTxn)).isFalse();
    }
}
