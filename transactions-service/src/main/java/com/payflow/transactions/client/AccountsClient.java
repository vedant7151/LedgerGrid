package com.payflow.transactions.client;

import com.payflow.transactions.security.JwtUtil;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Thin wrapper over the {@link RestClient} pointed at the Accounts service. A 4xx
 * from Accounts (e.g. insufficient funds -> 422) surfaces as a
 * {@link org.springframework.web.client.RestClientResponseException}, which the
 * transfer flow catches to mark the transaction FAILED.
 * Authenticates requests with accounts-service using a JWT signed with the shared secret.
 */
@Component
public class AccountsClient {

    private final RestClient restClient;
    private final JwtUtil jwtUtil;

    public AccountsClient(RestClient accountsClient, JwtUtil jwtUtil) {
        this.restClient = accountsClient;
        this.jwtUtil = jwtUtil;
    }

    public AccountView debit(UUID accountId, BigDecimal amount) {
        return call("/accounts/{id}/debit", accountId, amount);
    }

    public AccountView credit(UUID accountId, BigDecimal amount) {
        return call("/accounts/{id}/credit", accountId, amount);
    }

    private AccountView call(String path, UUID accountId, BigDecimal amount) {
        String token = jwtUtil.generate("transactions-service", "SERVICE");
        return restClient.post()
                .uri(path, accountId)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(new AmountBody(amount))
                .retrieve()
                .body(AccountView.class);
    }

    private record AmountBody(BigDecimal amount) {}
}
