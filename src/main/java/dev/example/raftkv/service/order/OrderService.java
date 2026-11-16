package dev.example.raftkv.service.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.example.raftkv.config.CommerceProperties;
import dev.example.raftkv.constants.RaftConstants;
import dev.example.raftkv.dto.kv.CompareAndSetValue;
import dev.example.raftkv.dto.order.CreateOrderRequest;
import dev.example.raftkv.dto.order.IdempotencyRecord;
import dev.example.raftkv.dto.order.OrderItem;
import dev.example.raftkv.dto.order.OrderRequestStatus;
import dev.example.raftkv.dto.order.OrderResponse;
import dev.example.raftkv.dto.order.OrderSubmission;
import dev.example.raftkv.exception.IdempotencyKeyConflictException;
import dev.example.raftkv.exception.InvalidOrderException;
import dev.example.raftkv.service.RaftConsensusService;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** E-commerce order creation coordinated by atomic Raft commands. */
@Service
public class OrderService {
    private static final String IDEMPOTENCY_PREFIX = "idempotency:orders:";
    private static final String ORDER_PREFIX = "orders:";

    private final RaftConsensusService raft;
    private final ObjectMapper objectMapper;
    private final CommerceProperties properties;

    public OrderService(
            RaftConsensusService raft,
            ObjectMapper objectMapper,
            CommerceProperties properties) {
        this.raft = raft;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public OrderSubmission createOrder(String idempotencyKey, CreateOrderRequest request) {
        validateIdempotencyKey(idempotencyKey);
        CreateOrderRequest canonicalRequest = canonicalize(request);
        String requestHash = hash(canonicalRequest);
        String recordKey = idempotencyKey(canonicalRequest.customerId(), idempotencyKey);

        Instant now = Instant.now();
        IdempotencyRecord proposed = new IdempotencyRecord(
                requestHash,
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                canonicalRequest.customerId(),
                "IN_PROGRESS",
                1,
                now,
                now.plus(Math.max(1, properties.getIdempotencyRetentionHours()), ChronoUnit.HOURS),
                null);

        RaftConsensusService.CommandResult begin = raft.execute(
                RaftConstants.IDEMPOTENCY_BEGIN_OPERATION, recordKey, writeJson(proposed));
        IdempotencyRecord current = readJson(begin.value(), IdempotencyRecord.class);
        if (!current.requestHash().equals(requestHash)) {
            throw new IdempotencyKeyConflictException();
        }
        if ("COMPLETED".equals(current.status())) {
            return new OrderSubmission(current.response(), true);
        }

        OrderResponse response = new OrderResponse(
                current.orderId(),
                current.customerId(),
                "CREATED",
                canonicalRequest.items(),
                current.createdAt());
        String orderKey = ORDER_PREFIX + current.orderId();
        RaftConsensusService.CommandResult orderWrite = raft.execute(
                RaftConstants.SET_IF_ABSENT_OPERATION, orderKey, writeJson(response));
        OrderResponse persistedOrder = readJson(orderWrite.value(), OrderResponse.class);

        IdempotencyRecord completed = new IdempotencyRecord(
                current.requestHash(),
                current.operationId(),
                current.orderId(),
                current.customerId(),
                "COMPLETED",
                current.version() + 1,
                current.createdAt(),
                current.expiresAt(),
                persistedOrder);
        String replacement = writeJson(completed);
        CompareAndSetValue compareAndSet = new CompareAndSetValue(begin.value(), replacement);
        RaftConsensusService.CommandResult completion = raft.execute(
                RaftConstants.COMPARE_AND_SET_OPERATION,
                recordKey,
                writeJson(compareAndSet));

        IdempotencyRecord finalRecord = readJson(completion.value(), IdempotencyRecord.class);
        if (!"COMPLETED".equals(finalRecord.status())) {
            throw new IllegalStateException("Order idempotency record did not reach COMPLETED state");
        }
        return new OrderSubmission(finalRecord.response(), !begin.applied());
    }

    public Optional<OrderResponse> findOrder(String orderId) {
        return raft.linearizableGet(ORDER_PREFIX + orderId)
                .map(value -> readJson(value, OrderResponse.class));
    }

    public Optional<OrderRequestStatus> findRequest(String customerId, String idempotencyKey) {
        if (customerId == null || customerId.isBlank()) {
            throw new InvalidOrderException("customerId is required to look up an idempotency key");
        }
        validateIdempotencyKey(idempotencyKey);
        return raft.linearizableGet(idempotencyKey(customerId, idempotencyKey))
                .map(value -> {
                    IdempotencyRecord record = readJson(value, IdempotencyRecord.class);
                    return new OrderRequestStatus(record.operationId(), record.status(),
                            record.orderId(), record.response(), record.expiresAt());
                });
    }

    private CreateOrderRequest canonicalize(CreateOrderRequest request) {
        if (request.items() == null || request.items().isEmpty()) {
            throw new InvalidOrderException("An order must contain at least one item");
        }
        List<OrderItem> items = request.items().stream()
                .map(item -> {
                    if (item.sku() == null || item.sku().isBlank() || item.quantity() <= 0) {
                        throw new InvalidOrderException("Each item needs a SKU and a positive quantity");
                    }
                    return new OrderItem(item.sku().trim(), item.quantity());
                })
                .sorted(Comparator.comparing(OrderItem::sku).thenComparingInt(OrderItem::quantity))
                .toList();
        if (request.customerId() == null || request.customerId().isBlank()) {
            throw new InvalidOrderException("customerId is required");
        }
        return new CreateOrderRequest(request.customerId().trim(), items);
    }

    private void validateIdempotencyKey(String key) {
        if (key == null || key.isBlank() || key.length() > 64) {
            throw new InvalidOrderException("Idempotency-Key must be between 1 and 64 characters");
        }
    }

    private String idempotencyKey(String customerId, String key) {
        return IDEMPOTENCY_PREFIX + customerId + ":" + key;
    }

    private String hash(CreateOrderRequest request) {
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(request);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            return HexFormat.of().formatHex(digest);
        } catch (JsonProcessingException | NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Unable to fingerprint order request", exception);
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize order state", exception);
        }
    }

    private <T> T readJson(String value, Class<T> type) {
        try {
            return objectMapper.readValue(value, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to read order state from Raft", exception);
        }
    }
}
