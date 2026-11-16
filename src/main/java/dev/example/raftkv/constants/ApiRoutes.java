package dev.example.raftkv.constants;

public final class ApiRoutes {
    public static final String KEY_VALUE = "/v1/kv/{key}";
    public static final String RAFT_STATUS = "/v1/raft/status";
    public static final String ORDERS = "/v1/orders";
    public static final String ORDER_BY_ID = "/v1/orders/{orderId}";
    public static final String ORDER_REQUEST_STATUS = "/v1/orders/requests/{idempotencyKey}";

    private ApiRoutes() { }
}
