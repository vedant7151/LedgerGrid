package com.payflow.ledger.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.payflow.ledger.domain.LedgerEntry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * On-demand aggregate report response for GET /ledger/report.
 * Contains period stats, flagged anomalies, category breakdown, and an optional
 * AI-generated executive narrative.
 */
public record LedgerReportResponse(
        String period,
        Instant since,
        Instant generatedAt,
        int totalTransactions,
        int flaggedCount,
        List<FlaggedEntryView> flaggedEntries,
        Map<String, Long> categoryBreakdown,
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String narrative
) {
    public record FlaggedEntryView(
            UUID id,
            UUID transactionId,
            UUID account,
            String reason,
            BigDecimal amount,
            String currency,
            Instant recordedAt
    ) {
        public static FlaggedEntryView from(LedgerEntry entry) {
            return new FlaggedEntryView(
                    entry.getId(),
                    entry.getTransactionId(),
                    entry.getFromAccount(),
                    entry.getAnomalyReason(),
                    entry.getAmount(),
                    entry.getCurrency(),
                    entry.getRecordedAt());
        }
    }
}
