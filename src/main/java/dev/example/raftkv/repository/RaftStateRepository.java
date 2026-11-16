package dev.example.raftkv.repository;

import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;

import java.util.List;
import java.util.Optional;

/** Storage boundary for Raft's durable term, vote, commit index, and log. */
public interface RaftStateRepository {
    record SavedState(long term, String votedFor, int commitIndex,
                      RaftSnapshot snapshot, List<RaftLogEntry> entries) { }

    Optional<SavedState> load();
    void save(long term, String votedFor, int commitIndex,
              RaftSnapshot snapshot, List<RaftLogEntry> entries);
}
