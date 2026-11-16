package dev.example.raftkv.dto.kv;

/** Payload for an atomic compare-and-set log command. */
public record CompareAndSetValue(String expectedValue, String replacementValue) { }
