package com.payflow.ledger.anomaly;

/**
 * The outcome of running rule-based anomaly detection on a ledger entry.
 */
public record AnomalyResult(boolean isAnomalous, String reason) {

    public static AnomalyResult normal() {
        return new AnomalyResult(false, null);
    }

    public static AnomalyResult anomalous(String reason) {
        return new AnomalyResult(true, reason);
    }
}
