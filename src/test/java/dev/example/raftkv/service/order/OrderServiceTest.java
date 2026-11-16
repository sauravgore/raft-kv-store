package dev.example.raftkv.service.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.example.raftkv.config.CommerceProperties;
import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.dto.order.CreateOrderRequest;
import dev.example.raftkv.dto.order.OrderItem;
import dev.example.raftkv.exception.IdempotencyKeyConflictException;
import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;
import dev.example.raftkv.repository.RaftStateRepository;
import dev.example.raftkv.service.RaftConsensusServiceImpl;
import dev.example.raftkv.transport.RaftPeerClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class OrderServiceTest {
    @Test
    void repeatedOrderRequestReturnsTheSameCompletedOrder() throws Exception {
        TestCluster cluster = new TestCluster();
        try {
            OrderService orders = cluster.orders();
            CreateOrderRequest request = request("buyer-1", "SKU-42", 2);

            var first = orders.createOrder("checkout-123", request);
            var retry = orders.createOrder("checkout-123", request);

            assertEquals(first.order().orderId(), retry.order().orderId());
            assertFalse(first.replayed());
            assertTrue(retry.replayed());
            assertEquals("COMPLETED", orders.findRequest("buyer-1", "checkout-123")
                    .orElseThrow().status());
        } finally {
            cluster.close();
        }
    }

    @Test
    void rejectsReusingTheSameKeyForDifferentOrderData() throws Exception {
        TestCluster cluster = new TestCluster();
        try {
            OrderService orders = cluster.orders();
            orders.createOrder("checkout-123", request("buyer-1", "SKU-42", 2));

            assertThrows(IdempotencyKeyConflictException.class,
                    () -> orders.createOrder("checkout-123", request("buyer-1", "SKU-42", 3)));
        } finally {
            cluster.close();
        }
    }

    @Test
    void concurrentRetriesCreateOnlyOneOrder() throws Exception {
        TestCluster cluster = new TestCluster();
        try {
            OrderService orders = cluster.orders();
            CreateOrderRequest request = request("buyer-1", "SKU-42", 2);
            CompletableFuture<String> first = CompletableFuture.supplyAsync(() ->
                    orders.createOrder("checkout-race", request).order().orderId());
            CompletableFuture<String> second = CompletableFuture.supplyAsync(() ->
                    orders.createOrder("checkout-race", request).order().orderId());

            assertEquals(first.get(3, TimeUnit.SECONDS), second.get(3, TimeUnit.SECONDS));
        } finally {
            cluster.close();
        }
    }

    @Test
    void completedOrderCanBeReadAfterServiceRecovery() throws Exception {
        TestCluster cluster = new TestCluster();
        String orderId;
        try {
            var submitted = cluster.orders().createOrder(
                    "checkout-123", request("buyer-1", "SKU-42", 1));
            orderId = submitted.order().orderId();
        } finally {
            cluster.closeServiceOnly();
        }

        cluster.recover();
        try {
            assertEquals(orderId, cluster.orders().findOrder(orderId).orElseThrow().orderId());
            assertTrue(cluster.orders().findRequest("buyer-1", "checkout-123")
                    .orElseThrow().response().orderId().equals(orderId));
        } finally {
            cluster.close();
        }
    }

    private static CreateOrderRequest request(String customerId, String sku, int quantity) {
        return new CreateOrderRequest(customerId, List.of(new OrderItem(sku, quantity)));
    }

    private static class TestCluster {
        private final RaftProperties raftConfig = new RaftProperties();
        private final MemoryStateRepository repository = new MemoryStateRepository();
        private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        private RaftConsensusServiceImpl raft;

        private TestCluster() throws InterruptedException {
            raftConfig.setElectionTimeoutMinMs(200);
            raftConfig.setElectionTimeoutMaxMs(201);
            raftConfig.setWriteCommitTimeoutMs(1000);
            raftConfig.setSnapshotThreshold(2);
            raft = new RaftConsensusServiceImpl(raftConfig, new NoPeerClient(), repository, mapper);
            Thread.sleep(220);
            raft.tick();
            assertTrue(raft.isLeader());
        }

        private OrderService orders() {
            return new OrderService(raft, mapper, new CommerceProperties());
        }

        private void recover() {
            raft = new RaftConsensusServiceImpl(raftConfig, new NoPeerClient(), repository, mapper);
            raft.recover();
            try {
                Thread.sleep(220);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(exception);
            }
            raft.tick();
            assertTrue(raft.isLeader());
        }

        private void closeServiceOnly() {
            raft.stopRpcPool();
        }

        private void close() {
            raft.stopRpcPool();
        }
    }

    private static class MemoryStateRepository implements RaftStateRepository {
        private SavedState saved = new SavedState(0, null, 0, null, List.of());

        @Override
        public Optional<SavedState> load() {
            return Optional.of(saved);
        }

        @Override
        public void save(long term, String votedFor, int commitIndex,
                         RaftSnapshot snapshot, List<RaftLogEntry> entries) {
            saved = new SavedState(term, votedFor, commitIndex, snapshot, new ArrayList<>(entries));
        }
    }

    private static class NoPeerClient implements RaftPeerClient {
        @Override
        public dev.example.raftkv.dto.raft.RaftMessages.VoteResponse requestVote(
                String peerId, dev.example.raftkv.dto.raft.RaftMessages.VoteRequest request) {
            return new dev.example.raftkv.dto.raft.RaftMessages.VoteResponse(request.term(), false);
        }

        @Override
        public dev.example.raftkv.dto.raft.RaftMessages.AppendResponse appendEntries(
                String peerId, dev.example.raftkv.dto.raft.RaftMessages.AppendRequest request) {
            return new dev.example.raftkv.dto.raft.RaftMessages.AppendResponse(
                    request.term(), false, request.prevLogIndex());
        }

        @Override
        public dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotResponse installSnapshot(
                String peerId, dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotRequest request) {
            return new dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotResponse(request.term(), false);
        }
    }
}
