package com.payflow.transactions.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.payflow.transactions.config.SecurityConfig;
import com.payflow.transactions.domain.Transaction;
import com.payflow.transactions.domain.TransactionStatus;
import com.payflow.transactions.service.DevFaultService;
import com.payflow.transactions.service.TransactionService;
import com.payflow.transactions.web.AuthController;
import com.payflow.transactions.web.DevController;
import com.payflow.transactions.web.TransactionController;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = {TransactionController.class, DevController.class, AuthController.class})
@Import({SecurityConfig.class, JwtAuthFilter.class, JwtUtil.class})
@TestPropertySource(properties = {
        "jwt.secret=ledgergrid-demo-secret-change-me-in-production-must-be-32chars"
})
class SecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtUtil jwtUtil;

    @MockitoBean
    private TransactionService transactionService;

    @MockitoBean
    private DevFaultService devFaultService;

    @Test
    void unauthenticatedRequestToTransactionsReturns401() throws Exception {
        mockMvc.perform(get("/transactions/" + UUID.randomUUID()))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void authLoginIsUnauthenticatedAndReturnsValidToken() throws Exception {
        mockMvc.perform(post("/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"admin\",\"password\":\"admin123\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").isString())
                .andExpect(jsonPath("$.username").value("admin"))
                .andExpect(jsonPath("$.role").value("ADMIN"));
    }

    @Test
    void authLoginWithInvalidPasswordReturns401() throws Exception {
        mockMvc.perform(post("/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"admin\",\"password\":\"wrongpassword\"}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void devSimulateErrorIsUnauthenticatedPerExclusionList() throws Exception {
        mockMvc.perform(post("/dev/simulate-error"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("armed"));
    }

    @Test
    void authenticatedRequestWithBearerTokenSucceeds() throws Exception {
        UUID txnId = UUID.randomUUID();
        Transaction txn = new Transaction();
        txn.setId(txnId);
        txn.setAmount(new BigDecimal("100"));
        txn.setFromAccount(UUID.randomUUID());
        txn.setToAccount(UUID.randomUUID());
        txn.setStatus(TransactionStatus.COMPLETED);
        txn.setCreatedAt(Instant.now());

        when(transactionService.get(txnId)).thenReturn(txn);

        String token = jwtUtil.generate("admin", "ADMIN");

        mockMvc.perform(get("/transactions/" + txnId)
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(txnId.toString()));
    }
}
