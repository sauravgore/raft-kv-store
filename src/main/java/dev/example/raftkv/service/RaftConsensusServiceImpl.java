package dev.example.raftkv.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.constants.RaftConstants;
import dev.example.raftkv.dto.kv.CompareAndSetValue;
import dev.example.raftkv.dto.order.IdempotencyRecord;
import dev.example.raftkv.enums.RaftRole;
import dev.example.raftkv.exception.NotLeaderException;
import dev.example.raftkv.exception.WriteCommitTimeoutException;
import dev.example.raftkv.repository.RaftStateRepository;
import dev.example.raftkv.transport.RaftPeerClient;
import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;
import dev.example.raftkv.dto.raft.RaftMessages.AppendRequest;
import dev.example.raftkv.dto.raft.RaftMessages.AppendResponse;
import dev.example.raftkv.dto.raft.RaftMessages.VoteRequest;
import dev.example.raftkv.dto.raft.RaftMessages.VoteResponse;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotRequest;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotResponse;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory Raft core. All state transitions are serialized by this object's monitor. */
@Component
public class RaftConsensusServiceImpl implements RaftConsensusService {
    private final RaftProperties config;
    private final RaftPeerClient rpcClient;
    private final RaftStateRepository stateRepository;
    private final ScheduledExecutorService rpcPool = Executors.newScheduledThreadPool(8);
    private final Map<String, String> peers;

    // Index zero is the Raft sentinel entry; real log entries start at index one.
    private final List<RaftLogEntry> log = new ArrayList<>();
    private final Map<Integer, Map<String, String>> partitions = new HashMap<>();
    private final Map<String, Integer> nextIndex = new HashMap<>();
    private final Map<String, Integer> matchIndex = new HashMap<>();
    private final Map<Integer, CompletableFuture<RaftConsensusService.CommandResult>> pendingWrites = new HashMap<>();
    private final ObjectMapper objectMapper;

    private volatile RaftRole role = RaftRole.FOLLOWER;
    private long currentTerm;
    private long electionDeadline;
    private long lastHeartbeat;
    private String votedFor;
    private String leaderId;
    private int commitIndex;
    private int lastApplied;
    private int lastIncludedIndex;
    private RaftSnapshot snapshot;

    public RaftConsensusServiceImpl(
            RaftProperties config,
            RaftPeerClient rpcClient,
            RaftStateRepository stateRepository,
            ObjectMapper objectMapper) {
        this.config = config;
        this.rpcClient = rpcClient;
        this.stateRepository = stateRepository;
        this.objectMapper = objectMapper;
        this.peers = config.peerMap();
        log.add(new RaftLogEntry(RaftConstants.SENTINEL_TERM, "", "", ""));
        resetElectionDeadline();
    }

    @PostConstruct
    public synchronized void recover() {
        stateRepository.load().ifPresent(saved -> {
            currentTerm = saved.term();
            votedFor = saved.votedFor();
            snapshot = saved.snapshot();
            lastIncludedIndex = snapshot == null ? 0 : snapshot.lastIncludedIndex();
            log.clear();
            long baseTerm = snapshot == null ? RaftConstants.SENTINEL_TERM : snapshot.lastIncludedTerm();
            log.add(new RaftLogEntry(baseTerm, "", "", ""));
            log.addAll(saved.entries());
            if (snapshot != null) {
                snapshot.partitions().forEach((partitionId, values) ->
                        partitions.put(partitionId, new HashMap<>(values)));
            }
            commitIndex = Math.max(lastIncludedIndex,
                    Math.min(saved.commitIndex(), lastLogIndex()));
            lastApplied = lastIncludedIndex;
            applyCommittedEntries();
            resetElectionDeadline();
        });
    }

    private void persist() {
        stateRepository.save(currentTerm, votedFor, commitIndex, snapshot, log.subList(1, log.size()));
    }

    private void resetElectionDeadline() {
        long minimum = Math.max(200, config.getElectionTimeoutMinMs());
        long maximum = Math.max(minimum + 1, config.getElectionTimeoutMaxMs());
        electionDeadline = System.currentTimeMillis()
                + ThreadLocalRandom.current().nextLong(minimum, maximum);
    }

    private long lastLogTerm() {
        return log.get(log.size() - 1).term();
    }

    private int lastLogIndex() {
        return lastIncludedIndex + log.size() - 1;
    }

    private int physicalIndex(int absoluteIndex) {
        return absoluteIndex - lastIncludedIndex;
    }

    private long termAt(int absoluteIndex) {
        return log.get(physicalIndex(absoluteIndex)).term();
    }

    private int majority() {
        return (peers.size() + 1) / 2 + 1;
    }

    public synchronized Map<String, Object> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodeId", config.getNodeId());
        result.put("role", role.name().toLowerCase());
        result.put("term", currentTerm);
        result.put("leaderId", leaderId == null ? "" : leaderId);
        result.put("commitIndex", commitIndex);
        result.put("lastLogIndex", lastLogIndex());
        result.put("lastIncludedIndex", lastIncludedIndex);
        result.put("members", peers.size() + 1);
        return result;
    }

    public synchronized Optional<String> get(String key) {
        return Optional.ofNullable(partitions.getOrDefault(partitionFor(key), Map.of()).get(key));
    }

    @Override
    public Optional<String> linearizableGet(String key) {
        execute(RaftConstants.READ_BARRIER_OPERATION, "read-barrier", "");
        return get(key);
    }

    public int partitionFor(String key) {
        return Math.floorMod(key.hashCode(), Math.max(1, config.getPartitionCount()));
    }

    public synchronized boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public synchronized String leaderId() {
        return leaderId;
    }

    public synchronized VoteResponse onRequestVote(VoteRequest request) {
        if (request.term() < currentTerm) {
            return new VoteResponse(currentTerm, false);
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), null);
            persist();
        }

        boolean candidateLogIsUpToDate = request.lastLogTerm() > lastLogTerm()
                || (request.lastLogTerm() == lastLogTerm()
                && request.lastLogIndex() >= lastLogIndex());
        boolean hasNotVotedOrAlreadyVotedForCandidate = votedFor == null
                || votedFor.equals(request.candidateId());
        boolean grantVote = hasNotVotedOrAlreadyVotedForCandidate && candidateLogIsUpToDate;

        if (grantVote) {
            votedFor = request.candidateId();
            leaderId = null;
            resetElectionDeadline();
            persist();
        }
        return new VoteResponse(currentTerm, grantVote);
    }

    public synchronized AppendResponse onAppendEntries(AppendRequest request) {
        if (request.term() < currentTerm) {
            return new AppendResponse(currentTerm, false, lastLogIndex());
        }
        if (request.term() > currentTerm || role != RaftRole.FOLLOWER) {
            becomeFollower(request.term(), request.leaderId());
        }

        leaderId = request.leaderId();
        resetElectionDeadline();
        if (!matchesPreviousLogEntry(request)) {
            return new AppendResponse(currentTerm, false, lastLogIndex());
        }

        appendNewEntries(request);
        updateCommitIndex(request.leaderCommit());
        persist();
        return new AppendResponse(currentTerm, true, lastLogIndex());
    }

    @Override
    public synchronized InstallSnapshotResponse onInstallSnapshot(InstallSnapshotRequest request) {
        if (request.term() < currentTerm) {
            return new InstallSnapshotResponse(currentTerm, false);
        }
        if (request.term() > currentTerm || role != RaftRole.FOLLOWER) {
            becomeFollower(request.term(), request.leaderId());
        }
        leaderId = request.leaderId();
        resetElectionDeadline();

        RaftSnapshot incoming = request.snapshot();
        if (incoming.lastIncludedIndex() <= commitIndex) {
            return new InstallSnapshotResponse(currentTerm, true);
        }

        snapshot = incoming;
        lastIncludedIndex = incoming.lastIncludedIndex();
        partitions.clear();
        incoming.partitions().forEach((partitionId, values) ->
                partitions.put(partitionId, new HashMap<>(values)));
        log.clear();
        log.add(new RaftLogEntry(incoming.lastIncludedTerm(), "", "", ""));
        commitIndex = Math.max(commitIndex, lastIncludedIndex);
        lastApplied = lastIncludedIndex;
        completeCommittedWrites();
        persist();
        return new InstallSnapshotResponse(currentTerm, true);
    }

    private boolean matchesPreviousLogEntry(AppendRequest request) {
        int previousIndex = request.prevLogIndex();
        return previousIndex >= lastIncludedIndex
                && previousIndex <= lastLogIndex()
                && termAt(previousIndex) == request.prevLogTerm();
    }

    private void appendNewEntries(AppendRequest request) {
        int index = request.prevLogIndex() + 1;
        for (RaftLogEntry entry : request.entries()) {
            if (index <= lastLogIndex() && termAt(index) != entry.term()) {
                truncateLogFrom(index);
            }
            if (index > lastLogIndex()) {
                log.add(entry);
            }
            index++;
        }
    }

    private void truncateLogFrom(int index) {
        while (lastLogIndex() >= index) {
            log.remove(log.size() - 1);
        }
    }

    private void updateCommitIndex(int leaderCommit) {
        if (leaderCommit > commitIndex) {
            commitIndex = Math.min(leaderCommit, lastLogIndex());
            applyCommittedEntries();
        }
    }

    private void becomeFollower(long term, String leader) {
        if (term > currentTerm) {
            votedFor = null;
        }
        currentTerm = term;
        role = RaftRole.FOLLOWER;
        leaderId = leader;
        resetElectionDeadline();
        persist();
    }

    @Scheduled(fixedDelay = 50)
    public void tick() {
        boolean shouldStartElection = false;
        boolean shouldSendHeartbeats = false;

        synchronized (this) {
            long now = System.currentTimeMillis();
            if (role == RaftRole.LEADER) {
                shouldSendHeartbeats = now - lastHeartbeat >= config.getHeartbeatIntervalMs();
                if (shouldSendHeartbeats) {
                    lastHeartbeat = now;
                }
            } else if (now >= electionDeadline) {
                role = RaftRole.CANDIDATE;
                currentTerm++;
                votedFor = config.getNodeId();
                leaderId = null;
                resetElectionDeadline();
                persist();
                shouldStartElection = true;
            }
        }

        if (shouldStartElection) {
            startElection();
        }
        if (shouldSendHeartbeats) {
            replicateAll();
        }
    }

    private void startElection() {
        final long electionTerm;
        final int lastLogIndex;
        final long lastLogTerm;
        synchronized (this) {
            electionTerm = currentTerm;
            lastLogIndex = lastLogIndex();
            lastLogTerm = lastLogTerm();
        }

        if (peers.isEmpty()) {
            synchronized (this) {
                if (role == RaftRole.CANDIDATE && currentTerm == electionTerm) {
                    becomeLeader();
                }
            }
            return;
        }

        AtomicInteger votesReceived = new AtomicInteger(1);
        VoteRequest request = new VoteRequest(
                electionTerm, config.getNodeId(), lastLogIndex, lastLogTerm);
        peers.forEach((peerId, address) -> rpcPool.submit(
                () -> requestVoteFromPeer(peerId, request, votesReceived)));
    }

    private void requestVoteFromPeer(
            String peerId,
            VoteRequest request,
            AtomicInteger votesReceived) {
        try {
            VoteResponse response = rpcClient.requestVote(peerId, request);

            synchronized (this) {
                if (response.term() > currentTerm) {
                    becomeFollower(response.term(), null);
                    return;
                }
                if (role != RaftRole.CANDIDATE || currentTerm != request.term()) {
                    return;
                }
            }

            if (response.voteGranted()
                    && votesReceived.incrementAndGet() >= majority()) {
                synchronized (this) {
                    if (role == RaftRole.CANDIDATE && currentTerm == request.term()) {
                        becomeLeader();
                    }
                }
            }
        } catch (Exception ignored) {
            // A failed peer is handled as a missed vote; the election timeout will retry.
        }
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        leaderId = config.getNodeId();
        int nextLogIndex = lastLogIndex() + 1;
        for (String peerId : peers.keySet()) {
            nextIndex.put(peerId, nextLogIndex);
            matchIndex.put(peerId, lastIncludedIndex);
        }
        lastHeartbeat = 0;
    }

    public int append(String operation, String key, String value) {
        return execute(operation, key, value).index();
    }

    @Override
    public CommandResult execute(String operation, String key, String value) {
        int newEntryIndex;
        CompletableFuture<CommandResult> committed;
        synchronized (this) {
            if (role != RaftRole.LEADER) {
                throw new NotLeaderException(leaderId);
            }

            log.add(new RaftLogEntry(currentTerm, operation, key, value));
            persist();
            newEntryIndex = lastLogIndex();
            committed = new CompletableFuture<>();
            pendingWrites.put(newEntryIndex, committed);
            matchIndex.put(config.getNodeId(), newEntryIndex);
            advanceCommitIndex();
        }

        try {
            committed.get(config.getWriteCommitTimeoutMs(), TimeUnit.MILLISECONDS);
            return committed.getNow(new CommandResult(newEntryIndex, true, value));
        } catch (TimeoutException exception) {
            synchronized (this) {
                pendingWrites.remove(newEntryIndex);
            }
            throw new WriteCommitTimeoutException(newEntryIndex, exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new WriteCommitTimeoutException(newEntryIndex, exception);
        } catch (ExecutionException exception) {
            throw new IllegalStateException("Write failed before commit", exception.getCause());
        }
    }

    private void replicateAll() {
        peers.keySet().forEach(peerId -> rpcPool.submit(() -> replicateToPeer(peerId)));
    }

    private void replicateToPeer(String peerId) {
        AppendRequest request;
        InstallSnapshotRequest snapshotRequest = null;
        synchronized (this) {
            if (role != RaftRole.LEADER) {
                return;
            }

            int nextLogIndex = nextIndex.getOrDefault(peerId, lastLogIndex() + 1);
            if (nextLogIndex <= lastIncludedIndex && snapshot != null) {
                snapshotRequest = new InstallSnapshotRequest(
                        currentTerm, config.getNodeId(), snapshot);
                request = null;
            } else {
                nextLogIndex = Math.max(lastIncludedIndex + 1,
                        Math.min(nextLogIndex, lastLogIndex() + 1));
                int previousLogIndex = nextLogIndex - 1;
            request = new AppendRequest(
                    currentTerm,
                    config.getNodeId(),
                    previousLogIndex,
                    termAt(previousLogIndex),
                    new ArrayList<>(log.subList(physicalIndex(nextLogIndex), log.size())),
                    commitIndex);
            }
        }

        try {
            if (snapshotRequest != null) {
                InstallSnapshotResponse response = rpcClient.installSnapshot(peerId, snapshotRequest);
                synchronized (this) {
                    if (role == RaftRole.LEADER && response.success()) {
                        int includedIndex = snapshotRequest.snapshot().lastIncludedIndex();
                        matchIndex.put(peerId, includedIndex);
                        nextIndex.put(peerId, includedIndex + 1);
                        advanceCommitIndex();
                    } else if (response.term() > currentTerm) {
                        becomeFollower(response.term(), null);
                    }
                }
                return;
            }
            AppendResponse response = rpcClient.appendEntries(peerId, request);
            handleAppendResponse(peerId, request, response);
        } catch (Exception ignored) {
            // A later heartbeat retries replication when this peer is reachable again.
        }
    }

    private synchronized void handleAppendResponse(
            String peerId,
            AppendRequest request,
            AppendResponse response) {
        if (response.term() > currentTerm) {
            becomeFollower(response.term(), null);
            return;
        }
        if (role != RaftRole.LEADER || request.term() != currentTerm) {
            return;
        }

        if (response.success()) {
            matchIndex.put(peerId, response.matchIndex());
            nextIndex.put(peerId, response.matchIndex() + 1);
            advanceCommitIndex();
        } else {
            int previousNextIndex = nextIndex.getOrDefault(peerId, 1);
            nextIndex.put(peerId, Math.max(1, previousNextIndex - 1));
        }
    }

    private void advanceCommitIndex() {
        for (int index = lastLogIndex(); index > commitIndex; index--) {
            if (termAt(index) != currentTerm) {
                continue;
            }

            int replicatedCopies = 1;
            for (String peerId : peers.keySet()) {
                if (matchIndex.getOrDefault(peerId, 0) >= index) {
                    replicatedCopies++;
                }
            }
            if (replicatedCopies >= majority()) {
                commitIndex = index;
                applyCommittedEntries();
                persist();
                break;
            }
        }
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            int appliedIndex = ++lastApplied;
            RaftLogEntry entry = log.get(physicalIndex(appliedIndex));
            StateMachineResult result = applyEntry(entry);
            CompletableFuture<CommandResult> pending = pendingWrites.remove(appliedIndex);
            if (pending != null) {
                pending.complete(new CommandResult(appliedIndex, result.applied(), result.value()));
            }
        }
        createSnapshotIfNeeded();
    }

    private void completeCommittedWrites() {
        pendingWrites.entrySet().removeIf(entry -> {
            if (entry.getKey() <= commitIndex) {
                entry.getValue().complete(new CommandResult(entry.getKey(), false, null));
                return true;
            }
            return false;
        });
    }

    private record StateMachineResult(boolean applied, String value) { }

    private StateMachineResult applyEntry(RaftLogEntry entry) {
        Map<String, String> partition = partitions.computeIfAbsent(
                partitionFor(entry.key()), ignored -> new HashMap<>());
        String currentValue = partition.get(entry.key());

        if (RaftConstants.SET_OPERATION.equals(entry.operation())) {
            partition.put(entry.key(), entry.value());
            return new StateMachineResult(true, entry.value());
        }
        if (RaftConstants.DELETE_OPERATION.equals(entry.operation())) {
            partition.remove(entry.key());
            return new StateMachineResult(true, null);
        }
        if (RaftConstants.READ_BARRIER_OPERATION.equals(entry.operation())) {
            return new StateMachineResult(true, currentValue);
        }
        if (RaftConstants.SET_IF_ABSENT_OPERATION.equals(entry.operation())) {
            if (currentValue != null) {
                return new StateMachineResult(false, currentValue);
            }
            partition.put(entry.key(), entry.value());
            return new StateMachineResult(true, entry.value());
        }
        if (RaftConstants.IDEMPOTENCY_BEGIN_OPERATION.equals(entry.operation())) {
            return applyIdempotencyBegin(entry, currentValue, partition);
        }
        if (RaftConstants.COMPARE_AND_SET_OPERATION.equals(entry.operation())) {
            return applyCompareAndSet(entry, currentValue, partition);
        }
        return new StateMachineResult(false, currentValue);
    }

    private StateMachineResult applyIdempotencyBegin(
            RaftLogEntry entry, String currentValue, Map<String, String> partition) {
        try {
            IdempotencyRecord proposed = objectMapper.readValue(entry.value(), IdempotencyRecord.class);
            if (currentValue == null) {
                partition.put(entry.key(), entry.value());
                return new StateMachineResult(true, entry.value());
            }

            IdempotencyRecord existing = objectMapper.readValue(currentValue, IdempotencyRecord.class);
            if (existing.expiresAt() != null && !existing.expiresAt().isAfter(proposed.createdAt())) {
                partition.put(entry.key(), entry.value());
                return new StateMachineResult(true, entry.value());
            }
            return new StateMachineResult(false, currentValue);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Invalid idempotency record in Raft log", exception);
        }
    }

    private StateMachineResult applyCompareAndSet(
            RaftLogEntry entry, String currentValue, Map<String, String> partition) {
        try {
            CompareAndSetValue command = objectMapper.readValue(entry.value(), CompareAndSetValue.class);
            if (!java.util.Objects.equals(currentValue, command.expectedValue())) {
                return new StateMachineResult(false, currentValue);
            }
            partition.put(entry.key(), command.replacementValue());
            return new StateMachineResult(true, command.replacementValue());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Invalid compare-and-set command in Raft log", exception);
        }
    }

    @PreDestroy
    public void stopRpcPool() {
        rpcPool.shutdownNow();
    }

    private void createSnapshotIfNeeded() {
        int threshold = Math.max(1, config.getSnapshotThreshold());
        if (lastApplied - lastIncludedIndex < threshold) {
            return;
        }

        int snapshotIndex = lastApplied;
        Map<Integer, Map<String, String>> partitionCopy = new HashMap<>();
        partitions.forEach((partitionId, values) ->
                partitionCopy.put(partitionId, new HashMap<>(values)));
        snapshot = new RaftSnapshot(snapshotIndex, termAt(snapshotIndex), partitionCopy);

        int entriesToRemove = snapshotIndex - lastIncludedIndex + 1;
        log.subList(0, entriesToRemove).clear();
        log.add(0, new RaftLogEntry(snapshot.lastIncludedTerm(), "", "", ""));
        lastIncludedIndex = snapshotIndex;
    }

    public synchronized void requireLeader() {
        if (role != RaftRole.LEADER) {
            throw new NotLeaderException(leaderId);
        }
    }

}
