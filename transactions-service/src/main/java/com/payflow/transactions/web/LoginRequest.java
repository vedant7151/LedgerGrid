package com.payflow.transactions.web;

/** Login request body for POST /auth/login. */
public record LoginRequest(String username, String password) {}
