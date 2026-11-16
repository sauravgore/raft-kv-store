package dev.example.raftkv.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;

/** Reads and atomically writes the node's durable Raft metadata and log. */
@Repository
public class JsonFileRaftStateRepository implements RaftStateRepository {
    private final Path stateFile;
    private final ObjectMapper objectMapper;

    public JsonFileRaftStateRepository(RaftProperties properties, ObjectMapper objectMapper) {
        this.stateFile = Path.of(properties.getDataDir(), properties.getNodeId(), "raft-state.json");
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void verifyStorageIsWritable() {
        Path directory = stateFile.getParent();
        try {
            Files.createDirectories(directory);
            Path probe = Files.createTempFile(directory, ".raft-kv-write-check-", ".tmp");
            Files.delete(probe);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Raft state directory is not writable: " + directory, exception);
        }
    }

    public Optional<RaftStateRepository.SavedState> load() {
        if (!Files.exists(stateFile)) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(stateFile.toFile(), RaftStateRepository.SavedState.class));
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to recover Raft state from " + stateFile, exception);
        }
    }

    public void save(long term, String votedFor, int commitIndex,
                     RaftSnapshot snapshot, List<RaftLogEntry> entries) {
        Path temporaryFile = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
        try {
            Files.createDirectories(stateFile.getParent());
            objectMapper.writeValue(temporaryFile.toFile(),
                    new RaftStateRepository.SavedState(
                            term, votedFor, commitIndex, snapshot, List.copyOf(entries)));
            try {
                Files.move(temporaryFile, stateFile, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicMoveNotSupported) {
                Files.move(temporaryFile, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to persist Raft state to " + stateFile, exception);
        }
    }
}
