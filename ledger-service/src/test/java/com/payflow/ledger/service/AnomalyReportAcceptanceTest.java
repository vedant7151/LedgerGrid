package com.payflow.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.payflow.ledger.anomaly.AnomalyDetector;
import com.payflow.ledger.anomaly.AnomalyReportNarrator;
import com.payflow.ledger.domain.LedgerEntry;
import com.payflow.ledger.repository.LedgerRepository;
import com.payflow.ledger.web.LedgerReportResponse;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Phase 2b acceptance: one normal transfer + one abnormally large transfer are recorded
 * through the real LedgerService + AnomalyDetector, then /ledger/report logic is run.
 * The large one must be flagged with a reason, with and without a Gemini key.
 */
class AnomalyReportAcceptanceTest {

    private final List<LedgerEntry> store = new ArrayList<>();
    private LedgerService ledgerService;
    private LedgerRepository repo;
    private final UUID sender = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        repo = mock(LedgerRepository.class);
        when(repo.save(any(LedgerEntry.class))).thenAnswer(inv -> {
            LedgerEntry e = inv.getArgument(0);
            if (e.getId() == null) {
                e.setId(UUID.randomUUID());
            }
            store.add(e);
            return e;
        });
        when(repo.findTopByOrderByChainIndexDesc()).thenAnswer(inv ->
                store.stream().max(Comparator.comparing(LedgerEntry::getChainIndex)));
        when(repo.findByFromAccountOrderByRecordedAtDesc(any())).thenAnswer(inv ->
                store.stream().filter(e -> e.getFromAccount().equals(inv.getArgument(0)))
                        .sorted(Comparator.comparing(LedgerEntry::getRecordedAt).reversed()).toList());
        when(repo.findByRecordedAtGreaterThanEqualOrderByRecordedAtDesc(any(Instant.class))).thenAnswer(inv ->
                store.stream().sorted(Comparator.comparing(LedgerEntry::getRecordedAt).reversed()).toList());
        ledgerService = new LedgerService(repo, new SimpleMeterRegistry(), new AnomalyDetector(repo));

        ledgerService.record(entry("100.00"));
        ledgerService.record(entry("5000.00"));
    }

    private LedgerEntry entry(String amount) {
        LedgerEntry e = new LedgerEntry();
        e.setTransactionId(UUID.randomUUID());
        e.setFromAccount(sender);
        e.setToAccount(UUID.randomUUID());
        e.setAmount(new BigDecimal(amount));
        e.setCurrency("INR");
        e.setCategory("transfer");
        return e;
    }

    private void assertLargeFlagged(LedgerReportResponse r) {
        assertThat(r.totalTransactions()).isEqualTo(2);
        assertThat(r.flaggedCount()).isEqualTo(1);
        assertThat(r.flaggedEntries().get(0).amount()).isEqualByComparingTo("5000.00");
        assertThat(r.flaggedEntries().get(0).reason()).isEqualTo("AMOUNT_OUTLIER");
        assertThat(r.flaggedEntries().get(0).account()).isEqualTo(sender);
    }

    @Test
    void largeTransferIsFlaggedWithoutGeminiKey() {
        var service = new LedgerReportService(repo, new AnomalyReportNarrator("", "gemini-flash-lite-latest"));
        LedgerReportResponse r = service.generateReport("24h");
        assertLargeFlagged(r);
        assertThat(r.narrative()).isNull();
    }

    @Test
    void largeTransferIsStillFlaggedWhenGeminiCallFails() {
        // A key is configured but invalid/unreachable: the call must fail open, not throw.
        var service = new LedgerReportService(repo, new AnomalyReportNarrator("invalid-key", "gemini-flash-lite-latest"));
        LedgerReportResponse r = service.generateReport("24h");
        assertLargeFlagged(r);
        assertThat(r.narrative()).isNull();
    }

    @Test
    void narrativeIsIncludedWhenGeminiReturnsOne() {
        var narrator = mock(AnomalyReportNarrator.class);
        when(narrator.generateNarrative(any(), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt(), any(), any()))
                .thenReturn("1 unusual transaction.");
        LedgerReportResponse r = new LedgerReportService(repo, narrator).generateReport("24h");
        assertLargeFlagged(r);
        assertThat(r.narrative()).isEqualTo("1 unusual transaction.");
    }
}
