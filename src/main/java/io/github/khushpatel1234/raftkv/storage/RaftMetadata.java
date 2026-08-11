package io.github.khushpatel1234.raftkv.storage;

/** Atomically persisted Raft safety metadata. A null vote means no vote in the term. */
public record RaftMetadata(long term, String votedFor, long commitIndex) {
    public RaftMetadata {
        if (term < 0) {
            throw new IllegalArgumentException("term cannot be negative");
        }
        if (votedFor != null && votedFor.isBlank()) {
            throw new IllegalArgumentException("votedFor cannot be blank");
        }
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex cannot be negative");
        }
    }

    public static RaftMetadata initial() {
        return new RaftMetadata(0L, null, 0L);
    }
}
