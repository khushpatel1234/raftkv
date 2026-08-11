package io.github.khushpatel1234.raftkv.raft;

import io.github.khushpatel1234.raftkv.core.RaftLogEntry;

import java.util.List;

public final class RaftRpc {
    private RaftRpc() {
    }

    public record RequestVoteRequest(
            long term, int candidateId, long lastLogIndex, long lastLogTerm) {
    }

    public record RequestVoteResponse(long term, boolean voteGranted) {
    }

    public record AppendEntriesRequest(
            long term,
            int leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<RaftLogEntry> entries,
            long leaderCommit) {
        public AppendEntriesRequest {
            entries = List.copyOf(entries);
        }
    }

    public record AppendEntriesResponse(
            long term, boolean success, long matchIndex, long conflictIndex) {
    }
}
