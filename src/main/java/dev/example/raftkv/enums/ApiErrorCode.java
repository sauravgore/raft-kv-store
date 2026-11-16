package dev.example.raftkv.enums;

/** Stable machine-readable codes returned by the HTTP API. */
public enum ApiErrorCode {
    NOT_LEADER("RAFT_NOT_LEADER"),
    WRITE_COMMIT_TIMEOUT("RAFT_WRITE_COMMIT_TIMEOUT"),
    IDEMPOTENCY_KEY_REUSED("IDEMPOTENCY_KEY_REUSED"),
    INVALID_ORDER("INVALID_ORDER"),
    VALIDATION_ERROR("VALIDATION_ERROR"),
    INVALID_REQUEST("INVALID_REQUEST"),
    INTERNAL_ERROR("INTERNAL_ERROR");

    private final String value;

    ApiErrorCode(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
