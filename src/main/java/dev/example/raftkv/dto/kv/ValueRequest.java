package dev.example.raftkv.dto.kv;

import jakarta.validation.constraints.NotBlank;

public record ValueRequest(@NotBlank String value) { }
