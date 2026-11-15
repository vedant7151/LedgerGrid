package com.payflow.transactions.service;

import com.payflow.transactions.client.AccountsClient;
import com.payflow.transactions.domain.CompensationEntry;
import com.payflow.transactions.domain.CompensationStatus;
import com.payflow.transactions.domain.Transaction;
import com.payflow.transactions.repository.CompensationEntryRepository;
import com.payflow.transactions.repository.TransactionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Retries refunds that failed inline (see {@link TransactionService}). Each PENDING row
 * is retried once it is due: {@code lastAttemptedAt + baseDelay * 2^attemptCount}
 * (5s, 10s, 20s, 40s, 80s with the defaults). After {@code maxAttempts} failed retries
 * the row becomes FAILED_PERMANENTLY and needs a human (the error log says so).
 *
 * <p>Caveat: the Accounts credit endpoint has no idempotency key. If a refund call
 * times out after Accounts applied it, a retry would refund twice. Fixing that needs
 * an idempotency key on the Accounts API (out of scope for Phase 7). Also assumes a
 * single transactions-service instance.
 */
@Component
public class CompensationRetryJob {

    private static final Logger log = LoggerFactory.getLogger(CompensationRetryJob.class);

    private final CompensationEntryRepository queue;
    private final TransactionRepository transactions;
    private final AccountsClient accountsClient;
    private final MeterRegistry meterRegistry;
    private final long baseDelayMs;
    private final int maxAttempts;

    public CompensationRetryJob(CompensationEntryRepository queue,
                                TransactionRepository transactions,
                                AccountsClient accountsClient,
                                MeterRegistry meterRegistry,
                                @Value("${compensation.base-delay-ms:5000}") long baseDelayMs,
                                @Value("${compensation.max-attempts:5}") int maxAttempts) {
        this.queue = queue;
        this.transactions = transactions;
        this.accountsClient = accountsClient;
        this.meterRegistry = meterRegistry;
        this.baseDelayMs = baseDelayMs;
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(fixedDelayString = "${compensation.poll-interval-ms:2000}")
    public void retryDue() {
        Instant now = Instant.now();
        List<CompensationEntry> pending = queue.findByStatusOrderByLastAttemptedAtAsc(CompensationStatus.PENDING);
        for (CompensationEntry entry : pending) {
            if (isDue(entry, now)) {
                retry(entry);
            }
        }
    }

    boolean isDue(CompensationEntry entry, Instant now) {
        long delay = baseDelayMs << Math.min(entry.getAttemptCount(), 20);
        return !entry.getLastAttemptedAt().plus(Duration.ofMillis(delay)).isAfter(now);
    }

    private void retry(CompensationEntry entry) {
        Transaction txn = transactions.findById(entry.getTransactionId()).orElse(null);
        if (txn == null) {
            fail(entry, "transaction " + entry.getTransactionId() + " not found");
            return;
        }
        entry.setAttemptCount(entry.getAttemptCount() + 1);
        entry.setLastAttemptedAt(Instant.now());
        try {
            accountsClient.credit(txn.getFromAccount(), txn.getAmount());
            entry.setStatus(CompensationStatus.RESOLVED);
            queue.save(entry);
            meterRegistry.counter("compensation.retries", "result", "resolved").increment();
            log.info("Compensation for transaction {} resolved on retry {}", txn.getId(), entry.getAttemptCount());
        } catch (Exception e) {
            entry.setReason(truncate(e.getMessage()));
            if (entry.getAttemptCount() >= maxAttempts) {
                fail(entry, truncate(e.getMessage()));
            } else {
                queue.save(entry);
                meterRegistry.counter("compensation.retries", "result", "retry_failed").increment();
                log.warn("Compensation retry {}/{} for transaction {} failed: {}",
                        entry.getAttemptCount(), maxAttempts, txn.getId(), e.getMessage());
            }
        }
    }

    private void fail(CompensationEntry entry, String reason) {
        entry.setReason(reason);
        entry.setStatus(CompensationStatus.FAILED_PERMANENTLY);
        queue.save(entry);
        meterRegistry.counter("compensation.retries", "result", "failed_permanently").increment();
        log.error("COMPENSATION FAILED PERMANENTLY for transaction {} after {} retries; manual refund required: {}",
                entry.getTransactionId(), entry.getAttemptCount(), reason);
    }

    private static String truncate(String s) {
        if (s == null) return "unknown error";
        return s.length() > 900 ? s.substring(0, 900) : s;
    }
}
