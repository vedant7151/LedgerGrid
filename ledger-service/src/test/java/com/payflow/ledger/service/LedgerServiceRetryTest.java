package com.payflow.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.payflow.ledger.anomaly.AnomalyDetector;
import com.payflow.ledger.anomaly.AnomalyResult;
import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

/** Deterministic (mock-based) tests of the chain-index retry loop in {@link LedgerService#record}. */
class LedgerServiceRetryTest {

    private LedgerRepository repo;
    private SimpleMeterRegistry meters;
    private LedgerService service;

    @BeforeEach
    void setUp() {
        repo = mock(LedgerRepository.class);
        meters = new SimpleMeterRegistry();
        AnomalyDetector detector = mock(AnomalyDetector.class);
        when(detector.detect(any(LedgerEntry.class))).thenReturn(AnomalyResult.normal());
        service = new LedgerService(repo, meters, detector);
    }

    private static LedgerEntry entry() {
        LedgerEntry e = new LedgerEntry();
        e.setTransactionId(UUID.randomUUID());
        e.setFromAccount(UUID.randomUUID());
        e.setToAccount(UUID.randomUUID());
        e.setAmount(new BigDecimal("5.00"));
        e.setCurrency("INR");
        return e;
    }

    private static LedgerEntry tail(long index) {
        LedgerEntry t = new LedgerEntry();
        t.setChainIndex(index);
        t.setEntryHash("f".repeat(64));
        return t;
    }

    @Test
    void retriesWithRefetchedTailAfterAChainIndexCollision() {
        // First read sees tail 4 (-> index 5); a competing writer takes 5; second read sees tail 5 (-> index 6).
        when(repo.findTopByOrderByChainIndexDesc())
                .thenReturn(Optional.of(tail(4)))
                .thenReturn(Optional.of(tail(5)));
        when(repo.save(any(LedgerEntry.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate chain_index"))
                .thenAnswer(inv -> inv.getArgument(0));
        when(repo.existsByTransactionId(any(UUID.class))).thenReturn(false);

        LedgerEntry saved = service.record(entry());

        assertThat(saved.getChainIndex()).isEqualTo(6L);
        assertThat(saved.getPreviousHash()).isEqualTo("f".repeat(64));
        assertThat(saved.getEntryHash()).isNotNull();
        verify(repo, times(2)).findTopByOrderByChainIndexDesc();
        assertThat(meters.counter("ledger.chain.retries").count()).isEqualTo(1.0);
    }

    @Test
    void failsLoudlyAfterTheAttemptCap() {
        when(repo.findTopByOrderByChainIndexDesc()).thenReturn(Optional.empty());
        when(repo.save(any(LedgerEntry.class))).thenThrow(new DataIntegrityViolationException("dup"));
        when(repo.existsByTransactionId(any(UUID.class))).thenReturn(false);

        assertThatThrownBy(() -> service.record(entry()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("5 attempts")
                .hasCauseInstanceOf(DataIntegrityViolationException.class);
        verify(repo, times(LedgerService.MAX_CHAIN_ATTEMPTS)).save(any(LedgerEntry.class));
    }

    @Test
    void duplicateTransactionIsNotRetried() {
        when(repo.findTopByOrderByChainIndexDesc()).thenReturn(Optional.empty());
        when(repo.save(any(LedgerEntry.class))).thenThrow(new DataIntegrityViolationException("dup txn"));
        when(repo.existsByTransactionId(any(UUID.class))).thenReturn(true);

        assertThatThrownBy(() -> service.record(entry()))
                .isInstanceOf(DataIntegrityViolationException.class);
        verify(repo, times(1)).save(any(LedgerEntry.class));
        assertThat(meters.counter("ledger.chain.retries").count()).isZero();
    }
}
