package dev.example.raftkv.transport.grpc;

import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.transport.RaftPeerClient;
import dev.example.raftkv.dto.raft.RaftMessages;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotRequest;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotResponse;
import dev.example.raftkv.grpc.AppendRequest;
import dev.example.raftkv.grpc.AppendResponse;
import dev.example.raftkv.grpc.RaftRpcGrpc;
import dev.example.raftkv.grpc.VoteRequest;
import dev.example.raftkv.grpc.VoteResponse;
import dev.example.raftkv.model.RaftSnapshot;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Maintains reusable gRPC channels to configured Raft peers. Peer targets use host:port. */
@Component
public class GrpcRaftPeerClient implements RaftPeerClient {
    private final Map<String, RaftRpcGrpc.RaftRpcBlockingStub> stubs = new ConcurrentHashMap<>();
    private final long timeoutMs;

    public GrpcRaftPeerClient(RaftProperties properties) {
        this.timeoutMs = properties.getRpcTimeoutMs();
        properties.peerMap().forEach((id, address) -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address)
                    .usePlaintext()
                    .build();
            stubs.put(id, RaftRpcGrpc.newBlockingStub(channel));
        });
    }

    public RaftMessages.VoteResponse requestVote(String peerId, RaftMessages.VoteRequest request) {
        VoteRequest grpcRequest = VoteRequest.newBuilder()
                .setTerm(request.term())
                .setCandidateId(request.candidateId())
                .setLastLogIndex(request.lastLogIndex())
                .setLastLogTerm(request.lastLogTerm())
                .build();
        VoteResponse response = stub(peerId).withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .requestVote(grpcRequest);
        return new RaftMessages.VoteResponse(response.getTerm(), response.getVoteGranted());
    }

    public RaftMessages.AppendResponse appendEntries(String peerId, RaftMessages.AppendRequest request) {
        AppendRequest.Builder grpcRequest = AppendRequest.newBuilder()
                .setTerm(request.term())
                .setLeaderId(request.leaderId())
                .setPrevLogIndex(request.prevLogIndex())
                .setPrevLogTerm(request.prevLogTerm())
                .setLeaderCommit(request.leaderCommit());
        request.entries().forEach(entry -> grpcRequest.addEntries(
                dev.example.raftkv.grpc.LogEntry.newBuilder()
                        .setTerm(entry.term())
                        .setOperation(entry.operation())
                        .setKey(entry.key())
                        .setValue(entry.value() == null ? "" : entry.value())
                        .build()));
        AppendResponse response = stub(peerId).withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .appendEntries(grpcRequest.build());
        return new RaftMessages.AppendResponse(
                response.getTerm(), response.getSuccess(), response.getMatchIndex());
    }

    @Override
    public InstallSnapshotResponse installSnapshot(String peerId, InstallSnapshotRequest request) {
        RaftSnapshot snapshot = request.snapshot();
        dev.example.raftkv.grpc.InstallSnapshotRequest.Builder grpcRequest =
                dev.example.raftkv.grpc.InstallSnapshotRequest.newBuilder()
                        .setTerm(request.term())
                        .setLeaderId(request.leaderId())
                        .setLastIncludedIndex(snapshot.lastIncludedIndex())
                        .setLastIncludedTerm(snapshot.lastIncludedTerm());
        snapshot.partitions().forEach((partitionId, values) -> {
            var partition = dev.example.raftkv.grpc.SnapshotPartition.newBuilder()
                    .setPartitionId(partitionId);
            values.forEach((key, value) -> partition.addEntries(
                    dev.example.raftkv.grpc.SnapshotKeyValue.newBuilder()
                            .setKey(key)
                            .setValue(value)
                            .build()));
            grpcRequest.addPartitions(partition.build());
        });

        dev.example.raftkv.grpc.InstallSnapshotResponse response =
                stub(peerId).withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                        .installSnapshot(grpcRequest.build());
        return new InstallSnapshotResponse(response.getTerm(), response.getSuccess());
    }

    private RaftRpcGrpc.RaftRpcBlockingStub stub(String peerId) {
        RaftRpcGrpc.RaftRpcBlockingStub stub = stubs.get(peerId);
        if (stub == null) {
            throw new IllegalArgumentException("No gRPC channel configured for peer " + peerId);
        }
        return stub;
    }
}
