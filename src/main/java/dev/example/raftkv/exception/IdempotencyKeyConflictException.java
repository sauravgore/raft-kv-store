package dev.example.raftkv.exception;

public class IdempotencyKeyConflictException extends RuntimeException {
    public IdempotencyKeyConflictException() {
        super("This idempotency key was already used with a different order request");
    }
}
