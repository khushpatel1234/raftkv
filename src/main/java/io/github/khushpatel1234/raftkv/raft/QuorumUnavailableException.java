package io.github.khushpatel1234.raftkv.raft;

public final class QuorumUnavailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public QuorumUnavailableException(String message) {
        super(message);
    }
}
