package com.payflow.ledger.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Poison-message handling for the {@code transactions} listener (Phase 8).
 *
 * <p>Without this, a record the consumer cannot process is retried forever and blocks
 * its partition, so every event behind it is stuck. Now:
 * <ul>
 *   <li>Records that cannot even be deserialized (malformed JSON) are not retryable: they go
 *       straight to {@code transactions.DLT} with their original bytes preserved.
 *       (This relies on the {@code ErrorHandlingDeserializer} configured in application.yml;
 *       a plain JsonDeserializer would throw before any error handler is reached.)</li>
 *   <li>Records whose processing throws are retried with exponential backoff
 *       (1s, 2s, 4s by default, 3 retries), then sent to the DLT and the consumer moves on.</li>
 * </ul>
 * The DLT record carries the failure in {@code kafka_dlt-exception-*} headers.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    public static final String DLT_TOPIC = "transactions.DLT";

    /** Same partition count as {@code transactions} (1): the recoverer keeps the source partition number. */
    @Bean
    NewTopic transactionsDeadLetterTopic() {
        return TopicBuilder.name(DLT_TOPIC).partitions(1).replicas(1).build();
    }

    /**
     * Producer used only for dead-lettering. Values are byte[] for undeserializable records
     * (original bytes are restored from the failure header) and JSON for ordinary objects.
     */
    @Bean
    KafkaTemplate<String, Object> deadLetterKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        // assignable=true + an ordered map: byte[] must be tried before the catch-all Object.class,
        // otherwise (exact-match mode) a TransactionEvent has no delegate and the DLT publish fails.
        Map<Class<?>, org.apache.kafka.common.serialization.Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, new JsonSerializer<>());
        DelegatingByTypeSerializer valueSerializer = new DelegatingByTypeSerializer(delegates, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config, new StringSerializer(), valueSerializer));
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> deadLetterKafkaTemplate,
                                          @Value("${ledger.dlt.initial-interval-ms:1000}") long initialIntervalMs,
                                          @Value("${ledger.dlt.max-retries:3}") int maxRetries) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterKafkaTemplate,
                (record, ex) -> {
                    log.error("Dead-lettering record {}-{}@{} to {}: {}", record.topic(), record.partition(),
                            record.offset(), DLT_TOPIC, ex.toString());
                    return new org.apache.kafka.common.TopicPartition(DLT_TOPIC, record.partition());
                });

        ExponentialBackOff backOff = new ExponentialBackOff(initialIntervalMs, 2.0);
        backOff.setMaxAttempts(maxRetries);          // Spring Kafka 3.x: number of retries after the first failure
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
