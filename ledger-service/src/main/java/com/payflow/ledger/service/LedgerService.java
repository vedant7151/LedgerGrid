package com.payflow.ledger.service;

import com.payflow.ledger.anomaly.AnomalyDetector;
import com.payflow.ledger.anomaly.AnomalyResult;
import com.payflow.ledger.chain.LedgerHasher;
import com.payflow.ledger.config.CacheConfig;
import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import com.payflow.ledger.web.ChainVerificationResult;
import com.payflow.ledger.web.LedgerEntryResponse;
import com.payflow.ledger.web.LedgerPage;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Read/write access to the ledger with Redis caching. Reads are cached; a write
 * (a new entry from the Kafka consumer) evicts the "all" list and both the
 * sender's and receiver's per-account lists, so a subsequent read always reflects
 * the freshly recorded transaction.
 */
@Service
public class LedgerService {

    private static final Logger log = LoggerFactory.getLogger(LedgerService.class);

    /** Max attempts to claim a chain index before giving up (Phase 6). */
    static final int MAX_CHAIN_ATTEMPTS = 5;

    private final LedgerRepository ledgerRepository;
    private final MeterRegistry meterRegistry;
    private final AnomalyDetector anomalyDetector;

    public LedgerService(LedgerRepository ledgerRepository,
                         MeterRegistry meterRegistry,
                         AnomalyDetector anomalyDetector) {
        this.ledgerRepository = ledgerRepository;
        this.meterRegistry = meterRegistry;
        this.anomalyDetector = anomalyDetector;
    }

    @Cacheable(cacheNames = CacheConfig.LEDGER_ALL)
    public LedgerPage getAll() {
        return page(ledgerRepository.findAllByOrderByRecordedAtDesc());
    }

    @Cacheable(cacheNames = CacheConfig.LEDGER_BY_ACCOUNT, key = "#accountId")
    public LedgerPage getByAccount(UUID accountId) {
        return page(ledgerRepository.findByFromAccountOrToAccountOrderByRecordedAtDesc(accountId, accountId));
    }

    public boolean exists(UUID transactionId) {
        return ledgerRepository.existsByTransactionId(transactionId);
    }

    /**
     * Link the entry into the hash chain and persist it, then invalidate the caches
     * it affects: the full list, plus the per-account lists for both accounts.
     *
     * <p>Chaining is done here (not in a JPA callback) so the exact values that get
     * stored are the ones that get hashed.
     */
    @Caching(evict = {
            @CacheEvict(cacheNames = CacheConfig.LEDGER_ALL, allEntries = true),
            @CacheEvict(cacheNames = CacheConfig.LEDGER_BY_ACCOUNT, key = "#entry.fromAccount"),
            @CacheEvict(cacheNames = CacheConfig.LEDGER_BY_ACCOUNT, key = "#entry.toAccount")
    })
    public LedgerEntry record(LedgerEntry entry) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            if (entry.getRecordedAt() == null) {
                entry.setRecordedAt(Instant.now().truncatedTo(ChronoUnit.MICROS));
            }

            // Rule-based anomaly detection: inline, non-blocking on failure (fail-open).
            // Independent of the chain position, so it runs once, outside the retry loop.
            try {
                AnomalyResult anomaly = anomalyDetector.detect(entry);
                entry.setAnomalous(anomaly.isAnomalous());
                entry.setAnomalyReason(anomaly.reason());
            } catch (Exception e) {
                log.warn("Anomaly detection failed for transaction {}; continuing: {}",
                        entry.getTransactionId(), e.getMessage());
                entry.setAnomalous(false);
                entry.setAnomalyReason(null);
            }

            return saveLinkedToChainTail(entry);
        } finally {
            sample.stop(meterRegistry.timer("ledger.write.latency"));
        }
    }

    /**
     * Read the chain tail, link the entry after it and save. If another writer took
     * the same chain index first, the unique constraint rejects our insert; we then
     * re-read the (new) tail, re-link, re-hash and try again, up to
     * {@link #MAX_CHAIN_ATTEMPTS} times, and fail loudly after that.
     *
     * <p>Not wrapped in an outer transaction on purpose: each {@code save} is its own
     * transaction, so a constraint violation rolls back cleanly and the next attempt
     * sees the other writer's committed row. Wrapping this in a caller transaction
     * would break that.
     */
    private LedgerEntry saveLinkedToChainTail(LedgerEntry entry) {
        for (int attempt = 1; ; attempt++) {
            LedgerEntry previous = ledgerRepository.findTopByOrderByChainIndexDesc().orElse(null);
            entry.setChainIndex(previous == null ? 0L : previous.getChainIndex() + 1);
            entry.setPreviousHash(previous == null ? LedgerHasher.GENESIS_HASH : previous.getEntryHash());
            entry.setEntryHash(LedgerHasher.hash(entry));
            try {
                return ledgerRepository.save(entry);
            } catch (DataIntegrityViolationException e) {
                // A duplicate transactionId (redelivered event) is also an integrity
                // violation, but retrying cannot fix it: surface it, do not loop.
                if (entry.getTransactionId() != null
                        && ledgerRepository.existsByTransactionId(entry.getTransactionId())) {
                    throw e;
                }
                if (attempt >= MAX_CHAIN_ATTEMPTS) {
                    log.error("Could not append ledger entry for transaction {} after {} attempts",
                            entry.getTransactionId(), attempt);
                    throw new IllegalStateException("Could not append ledger entry to the hash chain after "
                            + attempt + " attempts (chain index contention)", e);
                }
                meterRegistry.counter("ledger.chain.retries").increment();
                log.warn("Chain index {} already taken (attempt {}/{}); re-reading chain tail and retrying",
                        entry.getChainIndex(), attempt, MAX_CHAIN_ATTEMPTS);
                entry.setId(null); // the failed persist may have assigned an id; start fresh
                backoff(attempt);
            }
        }
    }

    /**
     * Exponential, fully-jittered pause (up to 20ms, 40ms, 80ms, 160ms) so colliding
     * writers spread out instead of retrying in lock-step.
     */
    private static void backoff(int attempt) {
        try {
            long ceilingMs = 10L << Math.min(attempt, 4);
            Thread.sleep(ThreadLocalRandom.current().nextLong(1, ceilingMs + 1));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Walk the chain from genesis and re-derive every hash. Detects three kinds of
     * tampering: an edited field (recomputed hash differs), a deleted entry (gap in
     * chainIndex, and the next entry's previousHash no longer matches), and a
     * reordered entry (chainIndex is part of the hash).
     */
    public ChainVerificationResult verifyChain() {
        List<LedgerEntry> chain = ledgerRepository.findAllByOrderByChainIndexAsc();
        String expectedPrevious = LedgerHasher.GENESIS_HASH;
        long expectedIndex = 0;
        long checked = 0;

        for (LedgerEntry entry : chain) {
            if (entry.getChainIndex() == null || entry.getEntryHash() == null) {
                return ChainVerificationResult.broken(checked,
                        "Entry is not part of the hash chain (missing chainIndex/entryHash).",
                        entry.getId(), entry.getChainIndex());
            }
            if (entry.getChainIndex() != expectedIndex) {
                return ChainVerificationResult.broken(checked,
                        "Chain break: expected chainIndex " + expectedIndex + " but found "
                                + entry.getChainIndex() + " — an entry was deleted or reordered.",
                        entry.getId(), entry.getChainIndex());
            }
            if (!expectedPrevious.equals(entry.getPreviousHash())) {
                return ChainVerificationResult.broken(checked,
                        "Chain break at index " + entry.getChainIndex()
                                + ": previousHash does not match the preceding entry's hash.",
                        entry.getId(), entry.getChainIndex());
            }
            String recomputed = LedgerHasher.hash(entry);
            if (!recomputed.equals(entry.getEntryHash())) {
                return ChainVerificationResult.broken(checked,
                        "Tampering detected at index " + entry.getChainIndex()
                                + ": stored hash does not match the entry's content.",
                        entry.getId(), entry.getChainIndex());
            }
            expectedPrevious = entry.getEntryHash();
            expectedIndex++;
            checked++;
        }
        return ChainVerificationResult.valid(checked);
    }

    private LedgerPage page(List<LedgerEntry> entries) {
        return new LedgerPage(entries.stream().map(LedgerEntryResponse::from).toList());
    }
}
