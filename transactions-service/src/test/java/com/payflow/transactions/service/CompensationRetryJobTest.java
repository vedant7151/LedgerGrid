package com.payflow.transactions.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.payflow.transactions.client.AccountView;
import com.payflow.transactions.client.AccountsClient;
import com.payflow.transactions.domain.CompensationEntry;
import com.payflow.transactions.domain.CompensationStatus;
import com.payflow.transactions.domain.Transaction;
import com.payflow.transactions.repository.CompensationEntryRepository;
import com.payflow.transactions.repository.TransactionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClientException;

class CompensationRetryJobTest {

    private static final long BASE_MS = 5_000;
    private static final int MAX = 3;

    private CompensationEntryRepository queue;
    private TransactionRepository transactions;
    private AccountsClient accounts;
    private SimpleMeterRegistry meters;
    private CompensationRetryJob job;
    private Transaction txn;

    @BeforeEach
    void setUp() {
        queue = mock(CompensationEntryRepository.class);
        transactions = mock(TransactionRepository.class);
        accounts = mock(AccountsClient.class);
        meters = new SimpleMeterRegistry();
        job = new CompensationRetryJob(queue, transactions, accounts, meters, BASE_MS, MAX);
        txn = new Transaction();
        txn.setId(UUID.randomUUID());
        txn.setFromAccount(UUID.randomUUID());
        txn.setAmount(new BigDecimal("75.00"));
        when(transactions.findById(txn.getId())).thenReturn(Optional.of(txn));
    }

    private CompensationEntry entry(int attempts, Instant lastAttempt) {
        CompensationEntry e = new CompensationEntry();
        e.setTransactionId(txn.getId());
        e.setReason("refund failed");
        e.setAttemptCount(attempts);
        e.setLastAttemptedAt(lastAttempt);
        when(queue.findByStatusOrderByLastAttemptedAtAsc(CompensationStatus.PENDING)).thenReturn(List.of(e));
        return e;
    }

    @Test
    void successfulRetryRefundsTheSenderAndResolves() {
        CompensationEntry e = entry(0, Instant.now().minusSeconds(60));
        when(accounts.credit(txn.getFromAccount(), txn.getAmount()))
                .thenReturn(new AccountView(txn.getFromAccount(), "A", BigDecimal.TEN, "INR", Instant.now()));

        job.retryDue();

        verify(accounts).credit(txn.getFromAccount(), txn.getAmount());
        assertThat(e.getStatus()).isEqualTo(CompensationStatus.RESOLVED);
        assertThat(e.getAttemptCount()).isEqualTo(1);
        verify(queue).save(e);
    }

    @Test
    void failedRetryStaysPendingAndBacksOff() {
        CompensationEntry e = entry(0, Instant.now().minusSeconds(60));
        when(accounts.credit(any(), any())).thenThrow(new RestClientException("still down"));

        job.retryDue();

        assertThat(e.getStatus()).isEqualTo(CompensationStatus.PENDING);
        assertThat(e.getAttemptCount()).isEqualTo(1);
        assertThat(e.getReason()).contains("still down");
        // just attempted, so the next retry (base * 2^1 = 10s) is not due yet
        assertThat(job.isDue(e, Instant.now())).isFalse();
        assertThat(job.isDue(e, Instant.now().plusSeconds(11))).isTrue();
    }

    @Test
    void backoffDoublesWithEachAttempt() {
        Instant t = Instant.parse("2026-01-01T00:00:00Z");
        for (int attempts = 0; attempts < 4; attempts++) {
            CompensationEntry e = new CompensationEntry();
            e.setAttemptCount(attempts);
            e.setLastAttemptedAt(t);
            long delaySec = (BASE_MS << attempts) / 1000;
            assertThat(job.isDue(e, t.plusSeconds(delaySec - 1))).isFalse();
            assertThat(job.isDue(e, t.plusSeconds(delaySec))).isTrue();
        }
    }

    @Test
    void entryNotYetDueIsLeftAlone() {
        entry(0, Instant.now()); // attempted just now, due in 5s
        job.retryDue();
        verify(accounts, never()).credit(any(), any());
    }

    @Test
    void givesUpAfterTheRetryCapAndMarksFailedPermanently() {
        CompensationEntry e = entry(MAX - 1, Instant.now().minusSeconds(3600));
        when(accounts.credit(eq(txn.getFromAccount()), any())).thenThrow(new RestClientException("down"));

        job.retryDue();

        assertThat(e.getAttemptCount()).isEqualTo(MAX);
        assertThat(e.getStatus()).isEqualTo(CompensationStatus.FAILED_PERMANENTLY);
        assertThat(meters.counter("compensation.retries", "result", "failed_permanently").count()).isEqualTo(1.0);
    }
}
