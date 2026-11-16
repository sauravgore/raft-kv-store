package dev.example.raftkv.dto.order;

public record OrderSubmission(OrderResponse order, boolean replayed) { }
