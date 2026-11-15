package com.payflow.ledger.anomaly;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnomalyDetectorTest {

    @Mock
    private LedgerRepository ledgerRepository;

    private AnomalyDetector detector;

    @BeforeEach
    void setUp() {
        detector = new AnomalyDetector(ledgerRepository);
    }

    private LedgerEntry createEntry(UUID account, BigDecimal amount, Instant recordedAt) {
        LedgerEntry entry = new LedgerEntry();
        entry.setId(UUID.randomUUID());
        entry.setTransactionId(UUID.randomUUID());
        entry.setFromAccount(account);
        entry.setToAccount(UUID.randomUUID());
        entry.setAmount(amount);
        entry.setRecordedAt(recordedAt);
        return entry;
    }

    @Test
    void normalFirstTransferIsNotAnomalous() {
        UUID account = UUID.randomUUID();
        when(ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account)).thenReturn(List.of());

        LedgerEntry entry = createEntry(account, new BigDecimal("100.00"), Instant.now());
        AnomalyResult result = detector.detect(entry);

        assertThat(result.isAnomalous()).isFalse();
        assertThat(result.reason()).isNull();
    }

    @Test
    void massiveFirstTransferFlagsAmountOutlier() {
        UUID account = UUID.randomUUID();
        when(ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account)).thenReturn(List.of());

        LedgerEntry entry = createEntry(account, new BigDecimal("60000.00"), Instant.now());
        AnomalyResult result = detector.detect(entry);

        assertThat(result.isAnomalous()).isTrue();
        assertThat(result.reason()).isEqualTo("AMOUNT_OUTLIER");
    }

    @Test
    void abnormalTransferAfterNormalTransferFlagsAmountOutlier() {
        UUID account = UUID.randomUUID();
        LedgerEntry prior = createEntry(account, new BigDecimal("100.00"), Instant.now().minusSeconds(120));
        when(ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account)).thenReturn(List.of(prior));

        // Normal amount is 100, current transfer is 5000 (50x larger, >= 1000)
        LedgerEntry current = createEntry(account, new BigDecimal("5000.00"), Instant.now());
        AnomalyResult result = detector.detect(current);

        assertThat(result.isAnomalous()).isTrue();
        assertThat(result.reason()).isEqualTo("AMOUNT_OUTLIER");
    }

    @Test
    void normalAmountWithinRollingRangeIsNotAnomalous() {
        UUID account = UUID.randomUUID();
        List<LedgerEntry> history = List.of(
                createEntry(account, new BigDecimal("100.00"), Instant.now().minusSeconds(300)),
                createEntry(account, new BigDecimal("120.00"), Instant.now().minusSeconds(240)),
                createEntry(account, new BigDecimal("110.00"), Instant.now().minusSeconds(180)),
                createEntry(account, new BigDecimal("95.00"), Instant.now().minusSeconds(120))
        );
        when(ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account)).thenReturn(history);

        LedgerEntry current = createEntry(account, new BigDecimal("115.00"), Instant.now());
        AnomalyResult result = detector.detect(current);

        assertThat(result.isAnomalous()).isFalse();
    }

    @Test
    void amountThreeStandardDeviationsAboveMeanFlagsAmountOutlier() {
        UUID account = UUID.randomUUID();
        List<LedgerEntry> history = List.of(
                createEntry(account, new BigDecimal("100.00"), Instant.now().minusSeconds(300)),
                createEntry(account, new BigDecimal("105.00"), Instant.now().minusSeconds(240)),
                createEntry(account, new BigDecimal("95.00"), Instant.now().minusSeconds(180)),
                createEntry(account, new BigDecimal("100.00"), Instant.now().minusSeconds(120))
        );
        when(ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account)).thenReturn(history);

        // Mean ~ 100, StdDev ~ 4. 2000 is way above 100 + 3 * 4 = 112
        LedgerEntry current = createEntry(account, new BigDecimal("2000.00"), Instant.now());
        AnomalyResult result = detector.detect(current);

        assertThat(result.isAnomalous()).isTrue();
        assertThat(result.reason()).isEqualTo("AMOUNT_OUTLIER");
    }

    @Test
    void moreThanFiveTransactionsInSixtySecondsFlagsFrequencySpike() {
        UUID account = UUID.randomUUID();
        Instant now = Instant.now();
        List<LedgerEntry> history = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            history.add(createEntry(account, new BigDecimal("50.00"), now.minusSeconds(i * 5)));
        }
        when(ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account)).thenReturn(history);

        LedgerEntry current = createEntry(account, new BigDecimal("50.00"), now);
        AnomalyResult result = detector.detect(current);

        assertThat(result.isAnomalous()).isTrue();
        assertThat(result.reason()).isEqualTo("FREQUENCY_SPIKE");
    }

    @Test
    void bothAmountOutlierAndFrequencySpikeAreFlaggedCombined() {
        UUID account = UUID.randomUUID();
        Instant now = Instant.now();
        List<LedgerEntry> history = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            history.add(createEntry(account, new BigDecimal("50.00"), now.minusSeconds(i * 5)));
        }
        when(ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account)).thenReturn(history);

        LedgerEntry current = createEntry(account, new BigDecimal("10000.00"), now);
        AnomalyResult result = detector.detect(current);

        assertThat(result.isAnomalous()).isTrue();
        assertThat(result.reason()).isEqualTo("AMOUNT_OUTLIER, FREQUENCY_SPIKE");
    }
}
