package dev.example.raftkv.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.dto.raft.RaftMessages.AppendRequest;
import dev.example.raftkv.dto.raft.RaftMessages.AppendResponse;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotRequest;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotResponse;
import dev.example.raftkv.dto.raft.RaftMessages.VoteRequest;
import dev.example.raftkv.dto.raft.RaftMessages.VoteResponse;
import dev.example.raftkv.exception.WriteCommitTimeoutException;
import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;
import dev.example.raftkv.repository.RaftStateRepository;
import dev.example.raftkv.transport.RaftPeerClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RaftConsensusServiceImplTest {
    @Test
    void singleNodeWriteReturnsOnlyAfterCommit() throws Exception {
        RaftProperties config = config("");
        config.setSnapshotThreshold(100);
        MemoryStateRepository repository = new MemoryStateRepository();
        RaftConsensusServiceImpl service = service(config, new FakePeerClient(false), repository);
        try {
            electSingleNode(service);

            int index = service.append("SET", "alpha", "one");

            assertEquals(1, index);
            assertEquals(Optional.of("one"), service.get("alpha"));
            assertEquals(1, service.status().get("commitIndex"));
        } finally {
            service.stopRpcPool();
        }
    }

    @Test
    void unavailableQuorumTimesOutAndDoesNotApplyWrite() throws Exception {
        RaftProperties config = config("node2=localhost:9092,node3=localhost:9093");
        config.setWriteCommitTimeoutMs(350);
        MemoryStateRepository repository = new MemoryStateRepository();
        RaftConsensusServiceImpl service = service(config, new FakePeerClient(true), repository);
        try {
            electClusterLeader(service);
            CompletableFuture<Integer> write = CompletableFuture.supplyAsync(
                    () -> service.append("SET", "uncommitted", "value"));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!write.isDone() && System.nanoTime() < deadline) {
                service.tick();
                Thread.sleep(15);
            }

            ExecutionException failure = assertThrows(ExecutionException.class, write::get);
            assertInstanceOf(WriteCommitTimeoutException.class, failure.getCause());
            assertTrue(service.get("uncommitted").isEmpty());
        } finally {
            service.stopRpcPool();
        }
    }

    @Test
    void compactedSnapshotRestoresStateAfterRestart() throws Exception {
        RaftProperties config = config("");
        config.setSnapshotThreshold(2);
        MemoryStateRepository repository = new MemoryStateRepository();
        RaftConsensusServiceImpl original = service(config, new FakePeerClient(false), repository);
        try {
            electSingleNode(original);
            original.append("SET", "first", "one");
            original.append("SET", "second", "two");

            assertNotNull(repository.saved.snapshot());
            assertEquals(2, repository.saved.snapshot().lastIncludedIndex());
            assertTrue(repository.saved.entries().isEmpty());
        } finally {
            original.stopRpcPool();
        }

        RaftConsensusServiceImpl recovered = service(config, new FakePeerClient(false), repository);
        try {
            recovered.recover();
            assertEquals(Optional.of("one"), recovered.get("first"));
            assertEquals(Optional.of("two"), recovered.get("second"));
            assertEquals(2, recovered.status().get("lastIncludedIndex"));
        } finally {
            recovered.stopRpcPool();
        }
    }

    private static RaftProperties config(String peers) {
        RaftProperties config = new RaftProperties();
        config.setNodeId("node1");
        config.setPeers(peers);
        config.setElectionTimeoutMinMs(200);
        config.setElectionTimeoutMaxMs(201);
        config.setHeartbeatIntervalMs(1);
        config.setWriteCommitTimeoutMs(1000);
        return config;
    }

    private static RaftConsensusServiceImpl service(
            RaftProperties config, RaftPeerClient peerClient, RaftStateRepository repository) {
        return new RaftConsensusServiceImpl(config, peerClient, repository, new ObjectMapper());
    }

    private static void electSingleNode(RaftConsensusServiceImpl service) throws InterruptedException {
        Thread.sleep(220);
        service.tick();
        assertTrue(service.isLeader());
    }

    private static void electClusterLeader(RaftConsensusServiceImpl service) throws Exception {
        Thread.sleep(220);
        service.tick();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!service.isLeader() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(service.isLeader(), "The node should receive a majority of votes");
    }

    private static class FakePeerClient implements RaftPeerClient {
        private final boolean failReplication;

        private FakePeerClient(boolean failReplication) {
            this.failReplication = failReplication;
        }

        @Override
        public VoteResponse requestVote(String peerId, VoteRequest request) {
            return new VoteResponse(request.term(), true);
        }

        @Override
        public AppendResponse appendEntries(String peerId, AppendRequest request) {
            if (failReplication) {
                throw new IllegalStateException("Simulated peer outage");
            }
            return new AppendResponse(request.term(), true,
                    request.prevLogIndex() + request.entries().size());
        }

        @Override
        public InstallSnapshotResponse installSnapshot(String peerId, InstallSnapshotRequest request) {
            return new InstallSnapshotResponse(request.term(), true);
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
}
