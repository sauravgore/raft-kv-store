package dev.example.raftkv.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "raft")
public class RaftProperties {
    private String nodeId = "node1";
    private String peers = "";
    private String httpPeers = "";
    private long electionTimeoutMinMs = 900;
    private long electionTimeoutMaxMs = 1600;
    private long heartbeatIntervalMs = 200;
    private long rpcTimeoutMs = 500;
    private int grpcPort = 9091;
    private String dataDir = "/data";
    private int partitionCount = 64;
    private long writeCommitTimeoutMs = 5000;
    private int snapshotThreshold = 100;

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getPeers() { return peers; }
    public void setPeers(String peers) { this.peers = peers; }
    public String getHttpPeers() { return httpPeers; }
    public void setHttpPeers(String httpPeers) { this.httpPeers = httpPeers; }

    public long getElectionTimeoutMinMs() { return electionTimeoutMinMs; }
    public void setElectionTimeoutMinMs(long value) { this.electionTimeoutMinMs = value; }

    public long getElectionTimeoutMaxMs() { return electionTimeoutMaxMs; }
    public void setElectionTimeoutMaxMs(long value) { this.electionTimeoutMaxMs = value; }

    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long value) { this.heartbeatIntervalMs = value; }

    public long getRpcTimeoutMs() { return rpcTimeoutMs; }
    public void setRpcTimeoutMs(long value) { this.rpcTimeoutMs = value; }
    public int getGrpcPort() { return grpcPort; }
    public void setGrpcPort(int value) { this.grpcPort = value; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String value) { this.dataDir = value; }
    public int getPartitionCount() { return partitionCount; }
    public void setPartitionCount(int value) { this.partitionCount = value; }
    public long getWriteCommitTimeoutMs() { return writeCommitTimeoutMs; }
    public void setWriteCommitTimeoutMs(long value) { this.writeCommitTimeoutMs = value; }
    public int getSnapshotThreshold() { return snapshotThreshold; }
    public void setSnapshotThreshold(int value) { this.snapshotThreshold = value; }

    public Map<String, String> peerMap() {
        Map<String, String> result = new LinkedHashMap<>();
        if (peers == null || peers.isBlank()) {
            return result;
        }

        for (String peer : peers.split(",")) {
            String[] pair = peer.trim().split("=", 2);
            if (pair.length == 2 && !pair[0].isBlank() && !pair[1].isBlank()
                    && !pair[0].trim().equals(nodeId)) {
                result.put(pair[0].trim(), pair[1].trim());
            }
        }
        return result;
    }

    public Map<String, String> httpPeerMap() {
        Map<String, String> result = new LinkedHashMap<>();
        if (httpPeers == null || httpPeers.isBlank()) {
            return result;
        }
        for (String peer : httpPeers.split(",")) {
            String[] pair = peer.trim().split("=", 2);
            if (pair.length == 2 && !pair[0].isBlank() && !pair[1].isBlank()) {
                result.put(pair[0].trim(), pair[1].trim());
            }
        }
        return result;
    }
}
