package dev.example.raftkv.dto.raft;

import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;

import java.util.List;

public final class RaftMessages {
    private RaftMessages() { }

    public record VoteRequest(
            long term,
            String candidateId,
            int lastLogIndex,
            long lastLogTerm) { }

    public record VoteResponse(long term, boolean voteGranted) { }

    public record AppendRequest(
            long term,
            String leaderId,
            int prevLogIndex,
            long prevLogTerm,
            List<RaftLogEntry> entries,
            int leaderCommit) { }

    public record AppendResponse(long term, boolean success, int matchIndex) { }
    public record InstallSnapshotRequest(long term, String leaderId, RaftSnapshot snapshot) { }
    public record InstallSnapshotResponse(long term, boolean success) { }
}
