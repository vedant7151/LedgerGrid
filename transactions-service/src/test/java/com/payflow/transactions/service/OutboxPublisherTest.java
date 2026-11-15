package com.payflow.transactions.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.payflow.transactions.domain.OutboxEvent;
import com.payflow.transactions.domain.OutboxStatus;
import com.payflow.transactions.event.TransactionEvent;
import com.payflow.transactions.repository.OutboxEventRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class OutboxPublisherTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private OutboxEventRepository repository;
    private KafkaTemplate<String, Object> kafka;
    private OutboxPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        kafka = mock(KafkaTemplate.class);
        publisher = new OutboxPublisher(repository, kafka, mapper);
    }

    private OutboxEvent pendingEvent() throws Exception {
        UUID id = UUID.randomUUID();
        TransactionEvent e = new TransactionEvent(id, UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("10.00"), "INR", "d", "m", Instant.now());
        OutboxEvent o = new OutboxEvent();
        o.setAggregateType("Transaction");
        o.setAggregateId(id.toString());
        o.setEventType("transaction.completed");
        o.setPayload(mapper.writeValueAsString(e));
        return o;
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishesPendingEventAndMarksItSent() throws Exception {
        OutboxEvent o = pendingEvent();
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxStatus.PENDING), any(Pageable.class)))
                .thenReturn(List.of(o));
        when(kafka.send(eq("transactions"), eq(o.getAggregateId()), any(TransactionEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        publisher.publishPending();

        verify(kafka).send(eq("transactions"), eq(o.getAggregateId()), any(TransactionEvent.class));
        assertThat(o.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(o.getSentAt()).isNotNull();
        verify(repository).save(o);
    }

    @Test
    void failedPublishLeavesEventPendingAndStopsTheBatch() throws Exception {
        OutboxEvent first = pendingEvent();
        OutboxEvent second = pendingEvent();
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxStatus.PENDING), any(Pageable.class)))
                .thenReturn(List.of(first, second));
        when(kafka.send(eq("transactions"), any(String.class), any(TransactionEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        publisher.publishPending();

        assertThat(first.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(second.getStatus()).isEqualTo(OutboxStatus.PENDING);
        verify(repository, never()).save(any(OutboxEvent.class));
        // Only the first event was attempted: order is preserved, nothing is skipped.
        verify(kafka, times(1)).send(eq("transactions"), any(String.class), any(TransactionEvent.class));
    }
}
