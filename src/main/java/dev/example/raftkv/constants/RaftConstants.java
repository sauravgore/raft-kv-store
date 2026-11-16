package dev.example.raftkv.constants;

public final class RaftConstants {
    public static final long SENTINEL_TERM = 0;
    public static final String SET_OPERATION = "SET";
    public static final String DELETE_OPERATION = "DELETE";
    public static final String SET_IF_ABSENT_OPERATION = "SET_IF_ABSENT";
    public static final String IDEMPOTENCY_BEGIN_OPERATION = "IDEMPOTENCY_BEGIN";
    public static final String COMPARE_AND_SET_OPERATION = "COMPARE_AND_SET";
    public static final String READ_BARRIER_OPERATION = "READ_BARRIER";

    private RaftConstants() { }
}
