package com.payflow.accounts.security;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.payflow.accounts.config.SecurityConfig;
import com.payflow.accounts.domain.Account;
import com.payflow.accounts.service.AccountService;
import com.payflow.accounts.web.AccountController;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = AccountController.class)
@Import({SecurityConfig.class, JwtAuthFilter.class, JwtUtil.class})
@TestPropertySource(properties = {
        "jwt.secret=ledgergrid-demo-secret-change-me-in-production-must-be-32chars"
})
class SecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AccountService accountService;

    private static final String SHARED_SECRET = "ledgergrid-demo-secret-change-me-in-production-must-be-32chars";

    @Test
    void unauthenticatedRequestToAccountsReturns401() throws Exception {
        mockMvc.perform(get("/accounts/" + UUID.randomUUID()))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void unauthenticatedRequestToActuatorIsPermitted() throws Exception {
        // Must NOT return 401 (unauthenticated requests to /actuator/** are permitted)
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().is(org.hamcrest.Matchers.not(401)));
    }

    @Test
    void authenticatedRequestWithValidSharedSecretTokenSucceeds() throws Exception {
        UUID accountId = UUID.randomUUID();
        Account acc = new Account();
        acc.setId(accountId);
        acc.setOwnerName("Alice");
        acc.setBalance(new BigDecimal("1000"));
        acc.setCurrency("INR");
        acc.setCreatedAt(Instant.now());

        when(accountService.getCached(accountId)).thenReturn(acc);

        // Generate token using the shared secret (as issued by transactions-service)
        String token = Jwts.builder()
                .subject("admin")
                .claim("role", "ADMIN")
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 3600000))
                .signWith(Keys.hmacShaKeyFor(SHARED_SECRET.getBytes(StandardCharsets.UTF_8)))
                .compact();

        mockMvc.perform(get("/accounts/" + accountId)
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(accountId.toString()))
                .andExpect(jsonPath("$.ownerName").value("Alice"));
    }

    @Test
    void requestWithInvalidTokenReturns401() throws Exception {
        mockMvc.perform(get("/accounts/" + UUID.randomUUID())
                        .header("Authorization", "Bearer invalid-garbage-token"))
                .andExpect(status().isUnauthorized());
    }
}
