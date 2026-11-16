package dev.example.raftkv.dto.order;

import java.time.Instant;

public record IdempotencyRecord(
        String requestHash,
        String operationId,
        String orderId,
        String customerId,
        String status,
        long version,
        Instant createdAt,
        Instant expiresAt,
        OrderResponse response) { }
