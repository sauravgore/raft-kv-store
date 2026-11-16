package dev.example.raftkv.exception;

public class NotLeaderException extends RuntimeException {
    private final String leader;

    public NotLeaderException(String leader) {
        super("This node is not the leader");
        this.leader = leader;
    }

    public String getLeader() {
        return leader;
    }
}
