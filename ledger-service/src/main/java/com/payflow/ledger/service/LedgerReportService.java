package com.payflow.ledger.service;

import com.payflow.ledger.anomaly.AnomalyReportNarrator;
import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import com.payflow.ledger.web.LedgerReportResponse;
import com.payflow.ledger.web.LedgerReportResponse.FlaggedEntryView;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

/**
 * Aggregates ledger transaction data over a requested timeframe, extracts anomalies
 * and categories, and enriches the aggregate report with an AI-generated narrative if available.
 */
@Service
public class LedgerReportService {

    private final LedgerRepository ledgerRepository;
    private final AnomalyReportNarrator reportNarrator;

    public LedgerReportService(LedgerRepository ledgerRepository,
                               AnomalyReportNarrator reportNarrator) {
        this.ledgerRepository = ledgerRepository;
        this.reportNarrator = reportNarrator;
    }

    public LedgerReportResponse generateReport(String sinceParam) {
        Duration duration = parseSinceDuration(sinceParam);
        Instant now = Instant.now();
        Instant cutoff = now.minus(duration);

        List<LedgerEntry> entries = ledgerRepository.findByRecordedAtGreaterThanEqualOrderByRecordedAtDesc(cutoff);
        int totalTransactions = entries.size();

        List<FlaggedEntryView> flaggedEntries = entries.stream()
                .filter(LedgerEntry::isAnomalous)
                .map(FlaggedEntryView::from)
                .toList();

        Map<String, Long> categoryBreakdown = entries.stream()
                .collect(Collectors.groupingBy(
                        e -> (e.getCategory() == null || e.getCategory().isBlank()) ? "other" : e.getCategory(),
                        Collectors.counting()
                ));

        String narrative = reportNarrator.generateNarrative(
                sinceParam,
                totalTransactions,
                flaggedEntries.size(),
                flaggedEntries,
                categoryBreakdown
        );

        return new LedgerReportResponse(
                sinceParam,
                cutoff,
                now,
                totalTransactions,
                flaggedEntries.size(),
                flaggedEntries,
                categoryBreakdown,
                narrative
        );
    }

    public static Duration parseSinceDuration(String since) {
        if (since == null || since.isBlank()) {
            return Duration.ofHours(24);
        }
        String trimmed = since.strip().toLowerCase();
        try {
            if (trimmed.endsWith("h")) {
                long hours = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));
                return Duration.ofHours(hours);
            }
            if (trimmed.endsWith("d")) {
                long days = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));
                return Duration.ofDays(days);
            }
            if (trimmed.endsWith("m")) {
                long minutes = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));
                return Duration.ofMinutes(minutes);
            }
            if (trimmed.endsWith("s")) {
                long seconds = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));
                return Duration.ofSeconds(seconds);
            }
            return Duration.parse(since);
        } catch (Exception e) {
            return Duration.ofHours(24);
        }
    }
}
