package dev.example.raftkv.service;

import dev.example.raftkv.dto.raft.RaftMessages.AppendRequest;
import dev.example.raftkv.dto.raft.RaftMessages.AppendResponse;
import dev.example.raftkv.dto.raft.RaftMessages.VoteRequest;
import dev.example.raftkv.dto.raft.RaftMessages.VoteResponse;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotRequest;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotResponse;

import java.util.Map;
import java.util.Optional;

/** Public operations offered by the Raft consensus layer. */
public interface RaftConsensusService {
    record CommandResult(int index, boolean applied, String value) { }

    Map<String, Object> status();
    Optional<String> get(String key);
    int partitionFor(String key);
    boolean isLeader();
    String leaderId();
    VoteResponse onRequestVote(VoteRequest request);
    AppendResponse onAppendEntries(AppendRequest request);
    InstallSnapshotResponse onInstallSnapshot(InstallSnapshotRequest request);
    int append(String operation, String key, String value);
    CommandResult execute(String operation, String key, String value);
    Optional<String> linearizableGet(String key);
}
