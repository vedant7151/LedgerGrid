package com.payflow.ledger.security;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.payflow.ledger.config.SecurityConfig;
import com.payflow.ledger.service.LedgerReportService;
import com.payflow.ledger.service.LedgerService;
import com.payflow.ledger.web.ChainVerificationResult;
import com.payflow.ledger.web.LedgerController;
import com.payflow.ledger.web.LedgerPage;
import com.payflow.ledger.web.LedgerReportResponse;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = LedgerController.class)
@Import({SecurityConfig.class, JwtAuthFilter.class, JwtUtil.class})
@TestPropertySource(properties = {
        "jwt.secret=ledgergrid-demo-secret-change-me-in-production-must-be-32chars"
})
class SecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private LedgerService ledgerService;

    @MockitoBean
    private LedgerReportService ledgerReportService;

    private static final String SHARED_SECRET = "ledgergrid-demo-secret-change-me-in-production-must-be-32chars";

    @Test
    void unauthenticatedRequestToLedgerReturns401() throws Exception {
        mockMvc.perform(get("/ledger"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void unauthenticatedRequestToLedgerReportReturns401() throws Exception {
        mockMvc.perform(get("/ledger/report"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void authenticatedRequestToLedgerReportSucceeds() throws Exception {
        when(ledgerReportService.generateReport("24h")).thenReturn(
                new LedgerReportResponse("24h", Instant.now(), Instant.now(), 0, 0, List.of(), Map.of(), null)
        );

        String token = Jwts.builder()
                .subject("admin")
                .claim("role", "ADMIN")
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 3600000))
                .signWith(Keys.hmacShaKeyFor(SHARED_SECRET.getBytes(StandardCharsets.UTF_8)))
                .compact();

        mockMvc.perform(get("/ledger/report")
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.period").value("24h"))
                .andExpect(jsonPath("$.totalTransactions").value(0));
    }

    @Test
    void unauthenticatedRequestToLedgerVerifyIsPermittedPerExclusionList() throws Exception {
        when(ledgerService.verifyChain()).thenReturn(ChainVerificationResult.valid(5));

        mockMvc.perform(get("/ledger/verify"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid").value(true))
                .andExpect(jsonPath("$.entriesChecked").value(5));
    }

    @Test
    void authenticatedRequestWithValidSharedSecretTokenSucceeds() throws Exception {
        when(ledgerService.getAll()).thenReturn(new LedgerPage(List.of()));

        // Token signed by transactions-service shared HMAC secret
        String token = Jwts.builder()
                .subject("admin")
                .claim("role", "ADMIN")
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 3600000))
                .signWith(Keys.hmacShaKeyFor(SHARED_SECRET.getBytes(StandardCharsets.UTF_8)))
                .compact();

        mockMvc.perform(get("/ledger")
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
    }

    @Test
    void requestWithInvalidTokenReturns401() throws Exception {
        mockMvc.perform(get("/ledger")
                        .header("Authorization", "Bearer invalid-tampered-token"))
                .andExpect(status().isUnauthorized());
    }
}
