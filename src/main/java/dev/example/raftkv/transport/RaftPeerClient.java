package dev.example.raftkv.transport;

import dev.example.raftkv.dto.raft.RaftMessages.AppendRequest;
import dev.example.raftkv.dto.raft.RaftMessages.AppendResponse;
import dev.example.raftkv.dto.raft.RaftMessages.VoteRequest;
import dev.example.raftkv.dto.raft.RaftMessages.VoteResponse;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotRequest;
import dev.example.raftkv.dto.raft.RaftMessages.InstallSnapshotResponse;

/** Transport-agnostic RPC boundary used by the Raft consensus service. */
public interface RaftPeerClient {
    VoteResponse requestVote(String peerId, VoteRequest request);
    AppendResponse appendEntries(String peerId, AppendRequest request);
    InstallSnapshotResponse installSnapshot(String peerId, InstallSnapshotRequest request);
}
