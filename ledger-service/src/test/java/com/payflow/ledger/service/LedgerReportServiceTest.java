package com.payflow.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.payflow.ledger.anomaly.AnomalyReportNarrator;
import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import com.payflow.ledger.web.LedgerReportResponse;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LedgerReportServiceTest {

    @Mock
    private LedgerRepository ledgerRepository;
    @Mock
    private AnomalyReportNarrator reportNarrator;

    private LedgerReportService reportService;

    @BeforeEach
    void setUp() {
        reportService = new LedgerReportService(ledgerRepository, reportNarrator);
    }

    private LedgerEntry createEntry(BigDecimal amount, String category, boolean isAnomalous, String reason) {
        LedgerEntry entry = new LedgerEntry();
        entry.setId(UUID.randomUUID());
        entry.setTransactionId(UUID.randomUUID());
        entry.setFromAccount(UUID.randomUUID());
        entry.setToAccount(UUID.randomUUID());
        entry.setAmount(amount);
        entry.setCurrency("INR");
        entry.setCategory(category);
        entry.setAnomalous(isAnomalous);
        entry.setAnomalyReason(reason);
        entry.setRecordedAt(Instant.now());
        return entry;
    }

    @Test
    void parseDurationHandlesVariousFormats() {
        assertThat(LedgerReportService.parseSinceDuration("24h")).isEqualTo(Duration.ofHours(24));
        assertThat(LedgerReportService.parseSinceDuration("7d")).isEqualTo(Duration.ofDays(7));
        assertThat(LedgerReportService.parseSinceDuration("30m")).isEqualTo(Duration.ofMinutes(30));
        assertThat(LedgerReportService.parseSinceDuration("60s")).isEqualTo(Duration.ofSeconds(60));
        assertThat(LedgerReportService.parseSinceDuration(null)).isEqualTo(Duration.ofHours(24));
        assertThat(LedgerReportService.parseSinceDuration("invalid")).isEqualTo(Duration.ofHours(24));
    }

    @Test
    void generateReportAggregatesTransactionsAndEnrichesWithNarrative() {
        LedgerEntry normal1 = createEntry(new BigDecimal("100.00"), "groceries", false, null);
        LedgerEntry normal2 = createEntry(new BigDecimal("150.00"), "food", false, null);
        LedgerEntry anomalous = createEntry(new BigDecimal("99999.00"), "shopping", true, "AMOUNT_OUTLIER");

        when(ledgerRepository.findByRecordedAtGreaterThanEqualOrderByRecordedAtDesc(any(Instant.class)))
                .thenReturn(List.of(normal1, normal2, anomalous));
        when(reportNarrator.generateNarrative(eq("24h"), eq(3), eq(1), anyList(), anyMap()))
                .thenReturn("1 anomalous transaction detected in the last 24h: an unusually high amount.");

        LedgerReportResponse report = reportService.generateReport("24h");

        assertThat(report.totalTransactions()).isEqualTo(3);
        assertThat(report.flaggedCount()).isEqualTo(1);
        assertThat(report.flaggedEntries()).hasSize(1);
        assertThat(report.flaggedEntries().get(0).reason()).isEqualTo("AMOUNT_OUTLIER");
        assertThat(report.categoryBreakdown()).containsEntry("groceries", 1L);
        assertThat(report.categoryBreakdown()).containsEntry("food", 1L);
        assertThat(report.categoryBreakdown()).containsEntry("shopping", 1L);
        assertThat(report.narrative()).isEqualTo("1 anomalous transaction detected in the last 24h: an unusually high amount.");
    }

    @Test
    void generateReportFailsOpenWhenNarratorReturnsNull() {
        LedgerEntry normal = createEntry(new BigDecimal("100.00"), "groceries", false, null);
        when(ledgerRepository.findByRecordedAtGreaterThanEqualOrderByRecordedAtDesc(any(Instant.class)))
                .thenReturn(List.of(normal));
        when(reportNarrator.generateNarrative(eq("24h"), eq(1), eq(0), anyList(), anyMap()))
                .thenReturn(null);

        LedgerReportResponse report = reportService.generateReport("24h");

        assertThat(report.totalTransactions()).isEqualTo(1);
        assertThat(report.flaggedCount()).isEqualTo(0);
        assertThat(report.flaggedEntries()).isEmpty();
        assertThat(report.narrative()).isNull();
    }
}
