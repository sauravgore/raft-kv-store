package dev.example.raftkv.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.example.raftkv.config.RaftProperties;
import dev.example.raftkv.model.RaftLogEntry;
import dev.example.raftkv.model.RaftSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonFileRaftStateRepositoryTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void verifiesDirectoryAndPersistsRaftState() {
        RaftProperties properties = new RaftProperties();
        properties.setDataDir(temporaryDirectory.toString());
        properties.setNodeId("node-test");
        JsonFileRaftStateRepository repository =
                new JsonFileRaftStateRepository(properties, new ObjectMapper());

        repository.verifyStorageIsWritable();
        RaftSnapshot snapshot = new RaftSnapshot(2, 1, Map.of(0, Map.of("color", "blue")));
        List<RaftLogEntry> remainingLog = List.of(new RaftLogEntry(2, "SET", "size", "large"));
        repository.save(2, "node-test", 3, snapshot, remainingLog);

        RaftStateRepository.SavedState restored = repository.load().orElseThrow();
        assertEquals(2, restored.term());
        assertEquals(3, restored.commitIndex());
        assertEquals(snapshot, restored.snapshot());
        assertEquals(remainingLog, restored.entries());
        assertTrue(temporaryDirectory.resolve("node-test/raft-state.json").toFile().exists());
    }
}
