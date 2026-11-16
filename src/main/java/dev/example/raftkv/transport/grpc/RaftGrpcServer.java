package dev.example.raftkv.transport.grpc;

import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;
import dev.example.raftkv.service.RaftConsensusService;
import dev.example.raftkv.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class RaftGrpcServer {
    private final Server server;

    public RaftGrpcServer(RaftProperties properties, RaftConsensusService raft) throws IOException {
        server = ServerBuilder.forPort(properties.getGrpcPort())
                .addService(new RaftService(raft))
                .build()
                .start();
    }

    @PreDestroy
    public void stop() {
        server.shutdown();
    }

    private static class RaftService extends RaftRpcGrpc.RaftRpcImplBase {
        private final RaftConsensusService raft;

        private RaftService(RaftConsensusService raft) {
            this.raft = raft;
        }

        @Override
        public void requestVote(VoteRequest request, StreamObserver<VoteResponse> observer) {
            var response = raft.onRequestVote(new dev.example.raftkv.dto.raft.RaftMessages.VoteRequest(
                    request.getTerm(), request.getCandidateId(),
                    request.getLastLogIndex(), request.getLastLogTerm()));
            observer.onNext(VoteResponse.newBuilder()
                    .setTerm(response.term())
                    .setVoteGranted(response.voteGranted())
                    .build());
            observer.onCompleted();
        }

        @Override
        public void appendEntries(AppendRequest request, StreamObserver<AppendResponse> observer) {
            var entries = request.getEntriesList().stream()
                    .map(entry -> new RaftLogEntry(
                            entry.getTerm(), entry.getOperation(), entry.getKey(), entry.getValue()))
                    .toList();
            var response = raft.onAppendEntries(new dev.example.raftkv.dto.raft.RaftMessages.AppendRequest(
                    request.getTerm(), request.getLeaderId(), request.getPrevLogIndex(),
                    request.getPrevLogTerm(), entries, request.getLeaderCommit()));
            observer.onNext(AppendResponse.newBuilder()
                    .setTerm(response.term())
                    .setSuccess(response.success())
                    .setMatchIndex(response.matchIndex())
                    .build());
            observer.onCompleted();
        }

        @Override
        public void installSnapshot(
                dev.example.raftkv.grpc.InstallSnapshotRequest request,
                StreamObserver<dev.example.raftkv.grpc.InstallSnapshotResponse> observer) {
            var partitions = new java.util.HashMap<Integer, java.util.Map<String, String>>();
            request.getPartitionsList().forEach(partition -> {
                var values = new java.util.HashMap<String, String>();
                partition.getEntriesList().forEach(entry -> values.put(entry.getKey(), entry.getValue()));
                partitions.put(partition.getPartitionId(), values);
            });
            var snapshot = new RaftSnapshot(
                    request.getLastIncludedIndex(), request.getLastIncludedTerm(), partitions);
            var response = raft.onInstallSnapshot(
                    new dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotRequest(
                            request.getTerm(), request.getLeaderId(), snapshot));
            observer.onNext(dev.example.raftkv.grpc.InstallSnapshotResponse.newBuilder()
                    .setTerm(response.term())
                    .setSuccess(response.success())
                    .build());
            observer.onCompleted();
        }
    }
}
