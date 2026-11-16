package dev.example.raftkv.exception;

public class WriteCommitTimeoutException extends RuntimeException {
    private final int logIndex;

    public WriteCommitTimeoutException(int logIndex, Throwable cause) {
        super("Write at log index " + logIndex + " did not reach majority commitment before timeout", cause);
        this.logIndex = logIndex;
    }

    public int getLogIndex() {
        return logIndex;
    }
}
