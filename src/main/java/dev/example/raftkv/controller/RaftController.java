package dev.example.raftkv.controller;

import dev.example.raftkv.constants.ApiRoutes;
import dev.example.raftkv.constants.RaftConstants;
import dev.example.raftkv.dto.kv.ErrorResponse;
import dev.example.raftkv.dto.kv.ValueRequest;
import dev.example.raftkv.exception.NotLeaderException;
import dev.example.raftkv.service.RaftConsensusService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class RaftController {
    private final RaftConsensusService raft;

    public RaftController(RaftConsensusService raft) {
        this.raft = raft;
    }

    @GetMapping(ApiRoutes.KEY_VALUE)
    public ResponseEntity<?> get(@PathVariable String key) {
        return raft.get(key)
                .<ResponseEntity<?>>map(value -> ResponseEntity.ok(Map.of(
                        "key", key, "value", value, "partition", raft.partitionFor(key))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping(ApiRoutes.KEY_VALUE)
    public ResponseEntity<?> put(
            @PathVariable String key,
            @Valid @RequestBody ValueRequest request) {
        return mutate(RaftConstants.SET_OPERATION, key, request.value());
    }

    @DeleteMapping(ApiRoutes.KEY_VALUE)
    public ResponseEntity<?> delete(@PathVariable String key) {
        return mutate(RaftConstants.DELETE_OPERATION, key, null);
    }

    private ResponseEntity<?> mutate(String operation, String key, String value) {
        int index = raft.append(operation, key, value);
        return ResponseEntity.ok().body(Map.of(
                "index", index, "status", "committed", "partition", raft.partitionFor(key)));
    }

    @GetMapping(ApiRoutes.RAFT_STATUS)
    public Map<String, Object> status() {
        return raft.status();
    }

}
