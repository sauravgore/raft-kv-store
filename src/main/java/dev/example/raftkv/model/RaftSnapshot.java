package dev.example.raftkv.model;

import java.util.Map;

/** Point-in-time state machine image plus the log position it represents. */
public record RaftSnapshot(
        int lastIncludedIndex,
        long lastIncludedTerm,
        Map<Integer, Map<String, String>> partitions) { }
