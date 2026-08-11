package io.github.khushpatel1234.raftkv.raft;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record RaftConfiguration(
        int nodeId,
        Map<Integer, InetSocketAddress> members,
        Duration electionTimeoutMin,
        Duration electionTimeoutMax,
        Duration heartbeatInterval,
        Duration rpcTimeout,
        Duration proposalTimeout) {

    public RaftConfiguration {
        members = Map.copyOf(new LinkedHashMap<>(Objects.requireNonNull(members, "members")));
        if (!members.containsKey(nodeId)) {
            throw new IllegalArgumentException("members must contain local node " + nodeId);
        }
        if (members.isEmpty()) {
            throw new IllegalArgumentException("a Raft group needs at least one member");
        }
        requirePositive(electionTimeoutMin, "electionTimeoutMin");
        requirePositive(electionTimeoutMax, "electionTimeoutMax");
        requirePositive(heartbeatInterval, "heartbeatInterval");
        requirePositive(rpcTimeout, "rpcTimeout");
        requirePositive(proposalTimeout, "proposalTimeout");
        if (electionTimeoutMax.compareTo(electionTimeoutMin) <= 0) {
            throw new IllegalArgumentException("electionTimeoutMax must be greater than electionTimeoutMin");
        }
        if (heartbeatInterval.compareTo(electionTimeoutMin) >= 0) {
            throw new IllegalArgumentException("heartbeatInterval must be shorter than the election timeout");
        }
    }

    public static RaftConfiguration defaults(int nodeId, Map<Integer, InetSocketAddress> members) {
        return new RaftConfiguration(
                nodeId,
                members,
                Duration.ofMillis(450),
                Duration.ofMillis(900),
                Duration.ofMillis(120),
                Duration.ofMillis(350),
                Duration.ofSeconds(3));
    }

    public int majority() {
        return members.size() / 2 + 1;
    }

    public Map<Integer, InetSocketAddress> peers() {
        var peers = new LinkedHashMap<>(members);
        peers.remove(nodeId);
        return Map.copyOf(peers);
    }

    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
