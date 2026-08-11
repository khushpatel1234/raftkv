package io.github.khushpatel1234.raftkv.storage;

import java.io.IOException;

/** Raised when a WAL contains corruption that cannot be treated as a torn tail write. */
public final class WalCorruptionException extends IOException {
    private static final long serialVersionUID = 1L;

    public WalCorruptionException(String message) {
        super(message);
    }

    public WalCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
