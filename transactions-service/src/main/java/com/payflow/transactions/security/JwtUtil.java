package com.payflow.transactions.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import javax.crypto.SecretKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * JWT utility for transactions-service.
 * This service is the only one that generates tokens (via /auth/login).
 * All three services share the same HMAC secret so tokens are accepted everywhere.
 */
@Component
public class JwtUtil {

    private static final long EXPIRY_MS = 60 * 60 * 1000L; // 1 hour

    private final SecretKey signingKey;

    public JwtUtil(@Value("${jwt.secret}") String secret) {
        this.signingKey = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    /** Generate a signed JWT for the given username and role. */
    public String generate(String username, String role) {
        long now = System.currentTimeMillis();
        return Jwts.builder()
                .subject(username)
                .claim("role", role)
                .issuedAt(new Date(now))
                .expiration(new Date(now + EXPIRY_MS))
                .signWith(signingKey)
                .compact();
    }

    /**
     * Parse and validate a token. Returns the Claims on success.
     * Throws {@link JwtException} if the token is invalid or expired.
     */
    public Claims validate(String token) {
        return Jwts.parser()
                .verifyWith(signingKey)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
