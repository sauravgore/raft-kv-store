package dev.example.raftkv.controller;

import dev.example.raftkv.constants.ApiRoutes;
import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.dto.order.CreateOrderRequest;
import dev.example.raftkv.dto.order.OrderRequestStatus;
import dev.example.raftkv.dto.order.OrderResponse;
import dev.example.raftkv.dto.order.OrderSubmission;
import dev.example.raftkv.exception.NotLeaderException;
import dev.example.raftkv.service.RaftConsensusService;
import dev.example.raftkv.service.order.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

@RestController
public class OrderController {
    private final OrderService orderService;
    private final RaftConsensusService raft;
    private final RaftProperties properties;

    public OrderController(OrderService orderService, RaftConsensusService raft, RaftProperties properties) {
        this.orderService = orderService;
        this.raft = raft;
        this.properties = properties;
    }

    @PostMapping(ApiRoutes.ORDERS)
    public ResponseEntity<OrderSubmission> createOrder(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request) {
        OrderSubmission result = orderService.createOrder(idempotencyKey, request);
        HttpStatus status = result.replayed() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(result);
    }

    @GetMapping(ApiRoutes.ORDER_BY_ID)
    public ResponseEntity<OrderResponse> getOrder(@PathVariable String orderId) {
        if (!raft.isLeader()) {
            URI leaderUri = leaderUri().path("/v1/orders/{orderId}")
                    .buildAndExpand(orderId).encode().toUri();
            return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT).location(leaderUri).build();
        }
        return orderService.findOrder(orderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping(ApiRoutes.ORDER_REQUEST_STATUS)
    public ResponseEntity<OrderRequestStatus> getRequestStatus(
            @PathVariable String idempotencyKey,
            @RequestParam(required = false) String customerId) {
        if (!raft.isLeader()) {
            URI leaderUri = leaderUri()
                    .path("/v1/orders/requests/{idempotencyKey}")
                    .queryParam("customerId", customerId)
                    .buildAndExpand(idempotencyKey).encode().toUri();
            return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT).location(leaderUri).build();
        }
        return orderService.findRequest(customerId, idempotencyKey)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private UriComponentsBuilder leaderUri() {
        String leaderAddress = properties.httpPeerMap().get(raft.leaderId());
        if (leaderAddress == null) {
            throw new NotLeaderException(raft.leaderId());
        }
        return UriComponentsBuilder.fromUriString(leaderAddress);
    }
}
