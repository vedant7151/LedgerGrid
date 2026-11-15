package com.payflow.ledger.web;

import com.payflow.ledger.service.LedgerReportService;
import com.payflow.ledger.service.LedgerService;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only access to the ledger. There are intentionally no write endpoints —
 * entries only ever arrive asynchronously via the Kafka consumer. Reads are served
 * from a Redis cache (see {@link LedgerService}).
 */
@RestController
@RequestMapping("/ledger")
public class LedgerController {

    private final LedgerService ledgerService;
    private final LedgerReportService ledgerReportService;

    public LedgerController(LedgerService ledgerService, LedgerReportService ledgerReportService) {
        this.ledgerService = ledgerService;
        this.ledgerReportService = ledgerReportService;
    }

    @GetMapping
    public List<LedgerEntryResponse> all() {
        return ledgerService.getAll().entries();
    }

    @GetMapping("/account/{id}")
    public List<LedgerEntryResponse> byAccount(@PathVariable UUID id) {
        return ledgerService.getByAccount(id).entries();
    }

    /**
     * Aggregated risk and anomaly detection report over a configurable timeframe (default 24h).
     * Protected by JWT auth like standard business endpoints.
     */
    @GetMapping("/report")
    public LedgerReportResponse report(@RequestParam(name = "since", defaultValue = "24h") String since) {
        return ledgerReportService.generateReport(since);
    }

    /**
     * Re-derives every entry's hash from its content and walks the chain. Returns
     * {@code valid: false} with the offending entry if anything was altered,
     * deleted, or reordered after it was written.
     */
    @GetMapping("/verify")
    public ChainVerificationResult verify() {
        return ledgerService.verifyChain();
    }
}
