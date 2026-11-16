package dev.example.raftkv.model;

public record RaftLogEntry(long term, String operation, String key, String value) { }
