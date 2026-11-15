package com.payflow.ledger.anomaly;

import com.google.genai.Client;
import com.google.genai.types.Content;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.Part;
import com.google.genai.types.ThinkingConfig;
import com.payflow.ledger.web.LedgerReportResponse.FlaggedEntryView;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Generates an executive natural-language narrative for an aggregate ledger report
 * using the Google Gemini API, following the exact fail-open pattern of
 * {@link com.payflow.ledger.categorizer.TransactionCategorizer}.
 *
 * <p>Never sends raw PII beyond what's already on the ledger. If the API key is not
 * configured or the LLM call fails/times out, this degrades gracefully by returning
 * {@code null}, leaving the report's structured JSON intact.
 */
@Component
public class AnomalyReportNarrator {

    private static final Logger log = LoggerFactory.getLogger(AnomalyReportNarrator.class);

    private static final String SYSTEM_INSTRUCTION =
            "You are a financial risk and anomaly analyst. Review the provided aggregate transaction "
            + "activity report and produce a concise 1-2 sentence executive narrative summarizing the "
            + "period's activity and highlighting any flagged anomalies. Respond with only plain narrative text.";

    private final Client client;   // null when no API key is configured
    private final String model;

    public AnomalyReportNarrator(@Value("${llm.api-key}") String apiKey,
                                 @Value("${llm.model}") String model) {
        this.model = model;
        this.client = (apiKey == null || apiKey.isBlank())
                ? null
                : Client.builder().apiKey(apiKey).build();
    }

    public String generateNarrative(String period,
                                    int totalTransactions,
                                    int flaggedCount,
                                    List<FlaggedEntryView> flaggedEntries,
                                    Map<String, Long> categoryBreakdown) {
        if (client == null) {
            // No API key configured: fail-open straight to no narrative
            return null;
        }

        try {
            return callGemini(period, totalTransactions, flaggedCount, flaggedEntries, categoryBreakdown);
        } catch (Exception e) {
            log.warn("Gemini narrative generation failed ({}); omitting narrative", e.toString());
            return null;
        }
    }

    private String callGemini(String period,
                              int totalTransactions,
                              int flaggedCount,
                              List<FlaggedEntryView> flaggedEntries,
                              Map<String, Long> categoryBreakdown) {
        StringBuilder flaggedSummary = new StringBuilder();
        for (FlaggedEntryView entry : flaggedEntries) {
            flaggedSummary.append("- Account ").append(entry.account())
                    .append(": ").append(entry.amount()).append(" ").append(entry.currency())
                    .append(" (reason: ").append(entry.reason()).append(")\n");
        }

        String prompt = """
                Aggregate Report:
                Period: %s
                Total Transactions: %d
                Flagged Anomalies: %d
                Flagged Details:
                %s
                Category Breakdown: %s

                Provide a 1-2 sentence executive narrative summary.""".formatted(
                period,
                totalTransactions,
                flaggedCount,
                flaggedSummary.length() == 0 ? "None" : flaggedSummary.toString(),
                categoryBreakdown.toString()
        );

        GenerateContentConfig config = GenerateContentConfig.builder()
                .thinkingConfig(ThinkingConfig.builder().thinkingLevel("MINIMAL").build())
                .systemInstruction(Content.fromParts(Part.fromText(SYSTEM_INSTRUCTION)))
                .build();

        GenerateContentResponse response = client.models.generateContent(model, prompt, config);

        String text = response.text();
        return (text == null || text.isBlank()) ? null : text.strip();
    }

    @PreDestroy
    void closeClient() {
        if (client != null) {
            client.close();
        }
    }
}
