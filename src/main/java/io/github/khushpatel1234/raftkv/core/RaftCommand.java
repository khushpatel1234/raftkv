package io.github.khushpatel1234.raftkv.core;

import java.io.Serial;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An immutable command replicated through the Raft log.
 *
 * <p>All byte arrays are copied on input and output. DELETE keeps its keys in
 * order so a multi-key RESP DEL can be represented by one log entry. GET is
 * deliberately absent: reads are served by {@link KeyValueStateMachine} and
 * are not appended to the replicated log.</p>
 */
public record RaftCommand(Type type, List<byte[]> keys, byte[] value) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final int MAGIC = 0x524B434D; // RKCM
    private static final byte VERSION = 1;
    private static final int MAX_ENCODED_SIZE = 64 * 1024 * 1024;

    public enum Type {
        SET(1),
        DELETE(2),
        NOOP(3);

        private final int wireCode;

        Type(int wireCode) {
            this.wireCode = wireCode;
        }

        private static Type fromWireCode(int wireCode) {
            return switch (wireCode) {
                case 1 -> SET;
                case 2 -> DELETE;
                case 3 -> NOOP;
                default -> throw new IllegalArgumentException("Unknown command type: " + wireCode);
            };
        }
    }

    public RaftCommand {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(keys, "keys");

        List<byte[]> copiedKeys = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            copiedKeys.add(Objects.requireNonNull(key, "key").clone());
        }
        keys = Collections.unmodifiableList(copiedKeys);
        value = value == null ? null : value.clone();

        switch (type) {
            case SET -> {
                if (keys.size() != 1 || value == null) {
                    throw new IllegalArgumentException("SET requires exactly one key and a value");
                }
            }
            case DELETE -> {
                if (keys.isEmpty() || value != null) {
                    throw new IllegalArgumentException("DELETE requires at least one key and no value");
                }
            }
            case NOOP -> {
                if (!keys.isEmpty() || value != null) {
                    throw new IllegalArgumentException("NOOP cannot contain keys or a value");
                }
            }
        }
    }

    public static RaftCommand set(byte[] key, byte[] value) {
        return new RaftCommand(Type.SET, List.of(Objects.requireNonNull(key, "key")), value);
    }

    public static RaftCommand delete(byte[]... keys) {
        Objects.requireNonNull(keys, "keys");
        return deleteKeys(Arrays.asList(keys));
    }

    public static RaftCommand deleteKeys(List<byte[]> keys) {
        return new RaftCommand(Type.DELETE, keys, null);
    }

    public static RaftCommand noop() {
        return new RaftCommand(Type.NOOP, List.of(), null);
    }

    /** Returns a deep defensive copy of the command keys. */
    @Override
    public List<byte[]> keys() {
        List<byte[]> copiedKeys = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            copiedKeys.add(key.clone());
        }
        return Collections.unmodifiableList(copiedKeys);
    }

    /** Returns the only key of a SET command. */
    public byte[] key() {
        if (keys.size() != 1) {
            throw new IllegalStateException(type + " does not have exactly one key");
        }
        return keys.getFirst().clone();
    }

    /** Returns a defensive value copy, or {@code null} for DELETE and NOOP. */
    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    /** Encodes this command using a deterministic, versioned binary format. */
    public byte[] encode() {
        long size = Integer.BYTES + 2L + Integer.BYTES + Integer.BYTES;
        for (byte[] key : keys) {
            size += Integer.BYTES + (long) key.length;
        }
        size += value == null ? 0L : value.length;
        if (size > MAX_ENCODED_SIZE) {
            throw new IllegalArgumentException("Encoded command exceeds " + MAX_ENCODED_SIZE + " bytes");
        }

        ByteBuffer output = ByteBuffer.allocate(Math.toIntExact(size));
        output.putInt(MAGIC);
        output.put(VERSION);
        output.put((byte) type.wireCode);
        output.putInt(keys.size());
        for (byte[] key : keys) {
            output.putInt(key.length).put(key);
        }
        output.putInt(value == null ? -1 : value.length);
        if (value != null) {
            output.put(value);
        }
        return output.array();
    }

    /** Decodes a command produced by {@link #encode()}. */
    public static RaftCommand decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length > MAX_ENCODED_SIZE) {
            throw new IllegalArgumentException("Encoded command exceeds " + MAX_ENCODED_SIZE + " bytes");
        }
        ByteBuffer input = ByteBuffer.wrap(encoded);
        requireRemaining(input, Integer.BYTES + 2 + Integer.BYTES, "command header");
        if (input.getInt() != MAGIC) {
            throw new IllegalArgumentException("Invalid command magic");
        }
        int version = Byte.toUnsignedInt(input.get());
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported command version: " + version);
        }
        Type type = Type.fromWireCode(Byte.toUnsignedInt(input.get()));
        int keyCount = input.getInt();
        if (keyCount < 0 || keyCount > input.remaining() / Integer.BYTES) {
            throw new IllegalArgumentException("Invalid command key count: " + keyCount);
        }
        List<byte[]> keys = new ArrayList<>(keyCount);
        for (int index = 0; index < keyCount; index++) {
            keys.add(readBytes(input, "key"));
        }
        requireRemaining(input, Integer.BYTES, "value length");
        int valueLength = input.getInt();
        byte[] value;
        if (valueLength == -1) {
            value = null;
        } else {
            value = readBytes(input, valueLength, "value");
        }
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after command");
        }
        return new RaftCommand(type, keys, value);
    }

    private static byte[] readBytes(ByteBuffer input, String field) {
        requireRemaining(input, Integer.BYTES, field + " length");
        return readBytes(input, input.getInt(), field);
    }

    private static byte[] readBytes(ByteBuffer input, int length, String field) {
        if (length < 0 || length > input.remaining()) {
            throw new IllegalArgumentException("Invalid " + field + " length: " + length);
        }
        byte[] bytes = new byte[length];
        input.get(bytes);
        return bytes;
    }

    private static void requireRemaining(ByteBuffer input, int required, String field) {
        if (input.remaining() < required) {
            throw new IllegalArgumentException("Truncated " + field);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RaftCommand other)
                || type != other.type
                || !Arrays.equals(value, other.value)
                || keys.size() != other.keys.size()) {
            return false;
        }
        for (int index = 0; index < keys.size(); index++) {
            if (!Arrays.equals(keys.get(index), other.keys.get(index))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        for (byte[] key : keys) {
            result = 31 * result + Arrays.hashCode(key);
        }
        return 31 * result + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "RaftCommand[type=" + type + ", keys=" + keys.size()
                + ", valueBytes=" + (value == null ? 0 : value.length) + ']';
    }
}
