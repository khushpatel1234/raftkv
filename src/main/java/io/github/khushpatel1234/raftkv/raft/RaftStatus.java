package io.github.khushpatel1234.raftkv.raft;

public record RaftStatus(
        int nodeId,
        RaftRole role,
        long term,
        Integer leaderId,
        long commitIndex,
        long lastApplied,
        long lastLogIndex,
        int clusterSize) {
}
