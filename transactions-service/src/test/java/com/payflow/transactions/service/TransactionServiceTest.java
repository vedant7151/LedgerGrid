package com.payflow.transactions.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.payflow.transactions.client.AccountView;
import com.payflow.transactions.client.AccountsClient;
import com.payflow.transactions.domain.Transaction;
import com.payflow.transactions.domain.TransactionStatus;
import com.payflow.transactions.domain.CompensationEntry;
import com.payflow.transactions.repository.CompensationEntryRepository;
import com.payflow.transactions.repository.TransactionRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestClientException;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {

    @Mock
    private TransactionRepository repository;
    @Mock
    private AccountsClient accountsClient;
    @Mock
    private TransactionCompletionService completionService;
    @Mock
    private CompensationEntryRepository compensationQueue;
    @Mock
    private DevFaultService devFaultService;
    @Spy
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();

    @InjectMocks
    private TransactionService service;

    @Test
    void rejectsTransferToSameAccount() {
        UUID account = UUID.randomUUID();

        assertThatThrownBy(() ->
                service.transfer(account, account, new BigDecimal("100"), null, null, null))
                .isInstanceOf(InvalidTransferException.class);
    }

    @Test
    void completedTransferMovesMoneyAndPublishesEvent() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("100");

        // Simulate JPA assigning the @GeneratedValue id on first save.
        when(repository.save(any(Transaction.class))).thenAnswer(inv -> {
            Transaction t = inv.getArgument(0);
            if (t.getId() == null) {
                t.setId(UUID.randomUUID());
            }
            return t;
        });
        // The completion service (status + outbox row in one tx) has its own unit test.
        when(completionService.complete(any(Transaction.class), eq("INR"))).thenAnswer(inv -> {
            Transaction t = inv.getArgument(0);
            t.setStatus(TransactionStatus.COMPLETED);
            return t;
        });
        when(accountsClient.debit(from, amount))
                .thenReturn(new AccountView(from, "Alice", new BigDecimal("900"), "INR", Instant.now()));
        when(accountsClient.credit(to, amount))
                .thenReturn(new AccountView(to, "Bob", new BigDecimal("100"), "INR", Instant.now()));

        Transaction result = service.transfer(from, to, amount, "Food delivery", "Swiggy", null);

        assertThat(result.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(result.getMerchant()).isEqualTo("Swiggy");
        // Completion (and its outbox event) is recorded exactly once; nothing publishes inline.
        verify(completionService).complete(any(Transaction.class), eq("INR"));
        assertThat(meterRegistry.counter("transactions.total", "status", "COMPLETED").count()).isEqualTo(1.0);
    }

    @Test
    void failedTransferRecordsFailedMetric() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("100");

        when(repository.save(any(Transaction.class))).thenAnswer(inv -> {
            Transaction t = inv.getArgument(0);
            if (t.getId() == null) {
                t.setId(UUID.randomUUID());
            }
            return t;
        });
        when(accountsClient.debit(from, amount))
                .thenThrow(new RestClientException("Connection refused"));

        assertThatThrownBy(() -> service.transfer(from, to, amount, "Test", null, null))
                .isInstanceOf(TransferFailedException.class);

        assertThat(meterRegistry.counter("transactions.total", "status", "FAILED").count()).isEqualTo(1.0);
        // A failed transfer must never produce an outbox event.
        verify(completionService, never()).complete(any(Transaction.class), any());
    }

    private void stubSaveAssignsId() {
        when(repository.save(any(Transaction.class))).thenAnswer(inv -> {
            Transaction t = inv.getArgument(0);
            if (t.getId() == null) {
                t.setId(UUID.randomUUID());
            }
            return t;
        });
    }

    @Test
    void failedRefundIsQueuedForRetryInsteadOfOnlyLogged() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("100");
        stubSaveAssignsId();
        when(accountsClient.debit(from, amount))
                .thenReturn(new AccountView(from, "Alice", new BigDecimal("900"), "INR", Instant.now()));
        // credit to the receiver fails, and so does the compensating refund to the sender
        when(accountsClient.credit(any(UUID.class), eq(amount)))
                .thenThrow(new RestClientException("accounts down"));

        assertThatThrownBy(() -> service.transfer(from, to, amount, "x", null, null))
                .isInstanceOf(TransferFailedException.class);

        org.mockito.ArgumentCaptor<CompensationEntry> cap = org.mockito.ArgumentCaptor.forClass(CompensationEntry.class);
        verify(compensationQueue).save(cap.capture());
        assertThat(cap.getValue().getTransactionId()).isNotNull();
        assertThat(cap.getValue().getReason()).contains("accounts down");
        assertThat(cap.getValue().getAttemptCount()).isZero();
        assertThat(meterRegistry.counter("compensation.queued").count()).isEqualTo(1.0);
    }

    @Test
    void successfulRefundIsNotQueued() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("100");
        stubSaveAssignsId();
        when(accountsClient.debit(from, amount))
                .thenReturn(new AccountView(from, "Alice", new BigDecimal("900"), "INR", Instant.now()));
        when(accountsClient.credit(to, amount)).thenThrow(new RestClientException("receiver unavailable"));
        when(accountsClient.credit(from, amount))
                .thenReturn(new AccountView(from, "Alice", new BigDecimal("1000"), "INR", Instant.now()));

        assertThatThrownBy(() -> service.transfer(from, to, amount, "x", null, null))
                .isInstanceOf(TransferFailedException.class);

        verify(compensationQueue, never()).save(any(CompensationEntry.class));
    }
}
