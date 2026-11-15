package com.payflow.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.payflow.ledger.anomaly.AnomalyDetector;
import com.payflow.ledger.anomaly.AnomalyResult;
import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LedgerServiceTest {

    @Mock
    private LedgerRepository ledgerRepository;
    @Mock
    private AnomalyDetector anomalyDetector;

    private MeterRegistry meterRegistry;
    private LedgerService ledgerService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        ledgerService = new LedgerService(ledgerRepository, meterRegistry, anomalyDetector);
    }

    @Test
    void recordPersistsEntryAndRecordsLatencyTimer() {
        when(ledgerRepository.findTopByOrderByChainIndexDesc()).thenReturn(Optional.empty());
        when(ledgerRepository.save(any(LedgerEntry.class))).thenAnswer(inv -> inv.getArgument(0));
        when(anomalyDetector.detect(any(LedgerEntry.class))).thenReturn(AnomalyResult.normal());

        LedgerEntry entry = new LedgerEntry();
        entry.setTransactionId(UUID.randomUUID());
        entry.setFromAccount(UUID.randomUUID());
        entry.setToAccount(UUID.randomUUID());
        entry.setAmount(new BigDecimal("150.00"));
        entry.setCurrency("INR");

        LedgerEntry saved = ledgerService.record(entry);

        assertThat(saved.getChainIndex()).isEqualTo(0L);
        assertThat(saved.getEntryHash()).isNotNull();
        assertThat(saved.isAnomalous()).isFalse();
        assertThat(meterRegistry.timer("ledger.write.latency").count()).isEqualTo(1L);
    }

    @Test
    void recordFlagsAnomalyWhenDetected() {
        when(ledgerRepository.findTopByOrderByChainIndexDesc()).thenReturn(Optional.empty());
        when(ledgerRepository.save(any(LedgerEntry.class))).thenAnswer(inv -> inv.getArgument(0));
        when(anomalyDetector.detect(any(LedgerEntry.class))).thenReturn(AnomalyResult.anomalous("AMOUNT_OUTLIER"));

        LedgerEntry entry = new LedgerEntry();
        entry.setTransactionId(UUID.randomUUID());
        entry.setFromAccount(UUID.randomUUID());
        entry.setToAccount(UUID.randomUUID());
        entry.setAmount(new BigDecimal("99999.00"));
        entry.setCurrency("INR");

        LedgerEntry saved = ledgerService.record(entry);

        assertThat(saved.isAnomalous()).isTrue();
        assertThat(saved.getAnomalyReason()).isEqualTo("AMOUNT_OUTLIER");
    }

    @Test
    void recordFailsOpenWhenAnomalyDetectorThrows() {
        when(ledgerRepository.findTopByOrderByChainIndexDesc()).thenReturn(Optional.empty());
        when(ledgerRepository.save(any(LedgerEntry.class))).thenAnswer(inv -> inv.getArgument(0));
        when(anomalyDetector.detect(any(LedgerEntry.class))).thenThrow(new RuntimeException("DB timeout"));

        LedgerEntry entry = new LedgerEntry();
        entry.setTransactionId(UUID.randomUUID());
        entry.setFromAccount(UUID.randomUUID());
        entry.setToAccount(UUID.randomUUID());
        entry.setAmount(new BigDecimal("100.00"));
        entry.setCurrency("INR");

        LedgerEntry saved = ledgerService.record(entry);

        assertThat(saved.isAnomalous()).isFalse();
        assertThat(saved.getAnomalyReason()).isNull();
        assertThat(meterRegistry.timer("ledger.write.latency").count()).isEqualTo(1L);
    }
}
