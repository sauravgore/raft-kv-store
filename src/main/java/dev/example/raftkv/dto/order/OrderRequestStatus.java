package dev.example.raftkv.dto.order;

import java.time.Instant;

public record OrderRequestStatus(
        String operationId,
        String status,
        String orderId,
        OrderResponse response,
        Instant expiresAt) { }
