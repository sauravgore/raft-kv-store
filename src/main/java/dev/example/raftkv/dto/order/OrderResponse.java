package dev.example.raftkv.dto.order;

import java.time.Instant;
import java.util.List;

public record OrderResponse(
        String orderId,
        String customerId,
        String status,
        List<OrderItem> items,
        Instant createdAt) { }
