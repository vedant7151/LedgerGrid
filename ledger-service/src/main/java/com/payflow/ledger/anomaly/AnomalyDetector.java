package com.payflow.ledger.anomaly;

import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Deterministic, rule-based anomaly detector for transactions.
 * Runs inline during ledger write without any external AI or network dependency.
 *
 * <p>Heuristics:
 * <ul>
 *   <li><b>AMOUNT_OUTLIER</b>: amount exceeds 3 standard deviations above the account's
 *       rolling average (or 3x for accounts with low variance/single prior transaction,
 *       or &gt;= 50,000 for brand-new accounts).</li>
 *   <li><b>FREQUENCY_SPIKE</b>: 5 or more transactions from the same sender account
 *       within a 60-second rolling window.</li>
 * </ul>
 */
@Component
public class AnomalyDetector {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetector.class);

    private static final int MAX_ROLLING_HISTORY = 50;
    private static final long FREQUENCY_WINDOW_SECONDS = 60;
    private static final int FREQUENCY_THRESHOLD = 5;

    private final LedgerRepository ledgerRepository;

    public AnomalyDetector(LedgerRepository ledgerRepository) {
        this.ledgerRepository = ledgerRepository;
    }

    public AnomalyResult detect(LedgerEntry current) {
        UUID account = current.getFromAccount();
        BigDecimal amount = current.getAmount();

        if (account == null || amount == null) {
            return AnomalyResult.normal();
        }

        List<LedgerEntry> rawHistory = ledgerRepository.findByFromAccountOrderByRecordedAtDesc(account);
        List<LedgerEntry> history = rawHistory == null ? List.of() : rawHistory.stream()
                .filter(e -> current.getId() == null || !current.getId().equals(e.getId()))
                .filter(e -> current.getTransactionId() == null || !current.getTransactionId().equals(e.getTransactionId()))
                .toList();

        boolean frequencySpike = checkFrequencySpike(current, history);
        boolean amountOutlier = checkAmountOutlier(amount, history);

        if (amountOutlier && frequencySpike) {
            log.info("Anomaly detected for transaction {}: AMOUNT_OUTLIER, FREQUENCY_SPIKE", current.getTransactionId());
            return AnomalyResult.anomalous("AMOUNT_OUTLIER, FREQUENCY_SPIKE");
        } else if (amountOutlier) {
            log.info("Anomaly detected for transaction {}: AMOUNT_OUTLIER", current.getTransactionId());
            return AnomalyResult.anomalous("AMOUNT_OUTLIER");
        } else if (frequencySpike) {
            log.info("Anomaly detected for transaction {}: FREQUENCY_SPIKE", current.getTransactionId());
            return AnomalyResult.anomalous("FREQUENCY_SPIKE");
        }

        return AnomalyResult.normal();
    }

    private boolean checkFrequencySpike(LedgerEntry current, List<LedgerEntry> history) {
        Instant currentTimestamp = current.getRecordedAt() != null ? current.getRecordedAt() : Instant.now();
        Instant windowStart = currentTimestamp.minusSeconds(FREQUENCY_WINDOW_SECONDS);

        long recentCount = history.stream()
                .filter(e -> e.getRecordedAt() != null && !e.getRecordedAt().isBefore(windowStart))
                .count();

        return recentCount >= FREQUENCY_THRESHOLD;
    }

    private boolean checkAmountOutlier(BigDecimal currentAmount, List<LedgerEntry> history) {
        double current = currentAmount.doubleValue();

        if (history.isEmpty()) {
            // First transaction for this account: flag if extraordinarily high
            return current >= 50000.0;
        }

        if (history.size() == 1) {
            double prev = history.get(0).getAmount().doubleValue();
            return prev > 0 && current >= prev * 3.0 && current >= 1000.0;
        }

        // 2 or more transactions: calculate rolling mean and standard deviation
        int sampleSize = Math.min(history.size(), MAX_ROLLING_HISTORY);
        double sum = 0.0;
        for (int i = 0; i < sampleSize; i++) {
            sum += history.get(i).getAmount().doubleValue();
        }
        double mean = sum / sampleSize;

        double sumSquaredDiff = 0.0;
        for (int i = 0; i < sampleSize; i++) {
            double diff = history.get(i).getAmount().doubleValue() - mean;
            sumSquaredDiff += diff * diff;
        }
        double stdDev = Math.sqrt(sumSquaredDiff / (sampleSize - 1));

        if (stdDev > 1.0) {
            return current > (mean + 3.0 * stdDev) && current > (mean * 2.0);
        } else {
            // Standard deviation is near zero (e.g. repeated fixed-amount transactions)
            return current >= (mean * 3.0) && current >= 1000.0;
        }
    }
}
