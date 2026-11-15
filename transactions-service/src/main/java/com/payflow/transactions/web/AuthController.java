package com.payflow.transactions.web;

import com.payflow.transactions.security.JwtUtil;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Issues JWTs for the demo's hardcoded users. This is intentionally simple —
 * there is no user database, no password hashing — the goal is to demonstrate
 * Bearer-token auth wiring across three services, not production auth.
 *
 * <p>Demo credentials (documented in IMPLEMENTATION_PLAN.md §3.1):
 * <ul>
 *   <li>admin / admin123  → role: ADMIN</li>
 *   <li>user  / user123   → role: USER</li>
 * </ul>
 *
 * This endpoint is explicitly excluded from JWT auth (see SecurityConfig).
 */
@RestController
@RequestMapping("/auth")
public class AuthController {

    private static final Map<String, String[]> USERS = Map.of(
            "admin", new String[]{"admin123", "ADMIN"},
            "user",  new String[]{"user123",  "USER"}
    );

    private final JwtUtil jwtUtil;

    public AuthController(JwtUtil jwtUtil) {
        this.jwtUtil = jwtUtil;
    }

    @PostMapping("/login")
    public ResponseEntity<Map<String, String>> login(@RequestBody LoginRequest request) {
        String[] creds = USERS.get(request.username());
        if (creds == null || !creds[0].equals(request.password())) {
            return ResponseEntity.status(401)
                    .body(Map.of("error", "Invalid username or password."));
        }
        String token = jwtUtil.generate(request.username(), creds[1]);
        return ResponseEntity.ok(Map.of(
                "token", token,
                "username", request.username(),
                "role", creds[1]
        ));
    }
}
