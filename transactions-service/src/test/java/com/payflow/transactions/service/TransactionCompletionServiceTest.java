package com.payflow.transactions.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.payflow.transactions.domain.OutboxEvent;
import com.payflow.transactions.domain.OutboxStatus;
import com.payflow.transactions.domain.Transaction;
import com.payflow.transactions.domain.TransactionStatus;
import com.payflow.transactions.repository.OutboxEventRepository;
import com.payflow.transactions.repository.TransactionRepository;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TransactionCompletionServiceTest {

    @Test
    void completesTransactionAndWritesPendingOutboxRow() {
        TransactionRepository txRepo = mock(TransactionRepository.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);
        when(txRepo.save(any(Transaction.class))).thenAnswer(inv -> inv.getArgument(0));
        TransactionCompletionService svc = new TransactionCompletionService(
                txRepo, outboxRepo, new ObjectMapper().registerModule(new JavaTimeModule()));

        Transaction t = new Transaction();
        t.setId(UUID.randomUUID());
        t.setFromAccount(UUID.randomUUID());
        t.setToAccount(UUID.randomUUID());
        t.setAmount(new BigDecimal("42.00"));
        t.setStatus(TransactionStatus.PENDING);

        Transaction result = svc.complete(t, "INR");

        assertThat(result.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        ArgumentCaptor<OutboxEvent> cap = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepo).save(cap.capture());
        OutboxEvent o = cap.getValue();
        assertThat(o.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(o.getAggregateId()).isEqualTo(t.getId().toString());
        assertThat(o.getEventType()).isEqualTo("transaction.completed");
        assertThat(o.getPayload()).contains(t.getId().toString()).contains("42.00");
    }
}
