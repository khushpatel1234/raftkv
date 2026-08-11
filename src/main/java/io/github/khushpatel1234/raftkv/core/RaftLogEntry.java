package io.github.khushpatel1234.raftkv.core;

import java.io.Serial;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/** A single immutable, one-based entry in the Raft log. */
public record RaftLogEntry(long index, long term, RaftCommand command) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final int MAGIC = 0x524B4C45; // RKLE
    private static final byte VERSION = 1;
    private static final int FIXED_SIZE = Integer.BYTES + 1 + Long.BYTES + Long.BYTES + Integer.BYTES;
    private static final int MAX_COMMAND_SIZE = 64 * 1024 * 1024;

    public RaftLogEntry {
        if (index < 1) {
            throw new IllegalArgumentException("index must be at least 1");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term cannot be negative");
        }
        Objects.requireNonNull(command, "command");
    }

    public byte[] encode() {
        byte[] commandBytes = command.encode();
        ByteBuffer output = ByteBuffer.allocate(FIXED_SIZE + commandBytes.length);
        output.putInt(MAGIC);
        output.put(VERSION);
        output.putLong(index);
        output.putLong(term);
        output.putInt(commandBytes.length);
        output.put(commandBytes);
        return output.array();
    }

    public static RaftLogEntry decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length < FIXED_SIZE) {
            throw new IllegalArgumentException("Truncated log entry");
        }
        ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.getInt() != MAGIC) {
            throw new IllegalArgumentException("Invalid log entry magic");
        }
        int version = Byte.toUnsignedInt(input.get());
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported log entry version: " + version);
        }
        long index = input.getLong();
        long term = input.getLong();
        int commandLength = input.getInt();
        if (commandLength < 0 || commandLength > MAX_COMMAND_SIZE || commandLength != input.remaining()) {
            throw new IllegalArgumentException("Invalid command length: " + commandLength);
        }
        byte[] commandBytes = new byte[commandLength];
        input.get(commandBytes);
        return new RaftLogEntry(index, term, RaftCommand.decode(commandBytes));
    }
}
