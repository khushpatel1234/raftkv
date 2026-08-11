package io.github.khushpatel1234.raftkv.raft;

public final class NotLeaderException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final Integer leaderId;

    public NotLeaderException(Integer leaderId) {
        super(leaderId == null ? "leader is not known" : "leader is node " + leaderId);
        this.leaderId = leaderId;
    }

    public Integer leaderId() {
        return leaderId;
    }
}
