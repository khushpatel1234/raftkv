package io.github.khushpatel1234.raftkv.server;

/** A client-safe command syntax error. */
public final class CommandParseException extends Exception {
    private static final long serialVersionUID = 1L;

    public CommandParseException(String message) {
        super(message);
    }
}
