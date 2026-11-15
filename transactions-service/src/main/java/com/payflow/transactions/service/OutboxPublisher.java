package com.payflow.transactions.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.transactions.domain.OutboxEvent;
import com.payflow.transactions.domain.OutboxStatus;
import com.payflow.transactions.event.TransactionEvent;
import com.payflow.transactions.repository.OutboxEventRepository;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The ONLY path by which transaction events reach Kafka. Polls PENDING outbox rows
 * oldest-first, publishes each and waits for the broker ack, then marks it SENT.
 * Any failure leaves the row PENDING (and stops the batch, preserving order) so the
 * next poll retries it: delivery is at-least-once, never dropped.
 *
 * <p>Assumes a single transactions-service instance; running several would need
 * row locking (FOR UPDATE SKIP LOCKED) to avoid duplicate publishes.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    static final String TOPIC = "transactions";
    private static final int BATCH_SIZE = 100;
    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxPublisher(OutboxEventRepository repository,
                           KafkaTemplate<String, Object> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}")
    public void publishPending() {
        List<OutboxEvent> batch = repository.findByStatusOrderByCreatedAtAsc(
                OutboxStatus.PENDING, PageRequest.of(0, BATCH_SIZE));
        for (OutboxEvent event : batch) {
            try {
                // Re-materialise the event so the wire format is identical to what the
                // JsonSerializer produced before the outbox (the ledger consumer is unchanged).
                TransactionEvent payload = objectMapper.readValue(event.getPayload(), TransactionEvent.class);
                kafkaTemplate.send(TOPIC, event.getAggregateId(), payload)
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Outbox event {} not published, will retry: {}", event.getId(), e.toString());
                return; // keep ordering: do not skip past a failed event
            }
            event.setStatus(OutboxStatus.SENT);
            event.setSentAt(Instant.now());
            repository.save(event);
            log.info("Published {} for {} via outbox", event.getEventType(), event.getAggregateId());
        }
    }
}
