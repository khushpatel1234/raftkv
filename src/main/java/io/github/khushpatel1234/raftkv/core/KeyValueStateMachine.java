package io.github.khushpatel1234.raftkv.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Thread-safe, binary-safe in-memory state machine used by committed Raft entries. */
public final class KeyValueStateMachine {
    private final ConcurrentMap<ByteArrayKey, byte[]> values = new ConcurrentHashMap<>();

    /** Returns a defensive copy, or {@code null} when the key is absent. */
    public byte[] get(byte[] key) {
        byte[] value = values.get(new ByteArrayKey(key));
        return value == null ? null : value.clone();
    }

    public Optional<byte[]> getOptional(byte[] key) {
        return Optional.ofNullable(get(key));
    }

    public void set(byte[] key, byte[] value) {
        values.put(new ByteArrayKey(key), Objects.requireNonNull(value, "value").clone());
    }

    public boolean delete(byte[] key) {
        return values.remove(new ByteArrayKey(key)) != null;
    }

    /** Deletes keys in order and counts each actual removal, matching Redis DEL. */
    public long delete(List<byte[]> keys) {
        Objects.requireNonNull(keys, "keys");
        long deleted = 0;
        for (byte[] key : keys) {
            if (delete(key)) {
                deleted++;
            }
        }
        return deleted;
    }

    /** Applies a committed command and returns its deterministic affected count. */
    public long apply(RaftCommand command) {
        Objects.requireNonNull(command, "command");
        return switch (command.type()) {
            case SET -> {
                set(command.key(), command.value());
                yield 1L;
            }
            case DELETE -> delete(command.keys());
            case NOOP -> 0L;
        };
    }

    public int size() {
        return values.size();
    }

    public void clear() {
        values.clear();
    }

    /** Returns a deep-copy snapshot suitable for diagnostics and tests. */
    public List<Entry> snapshot() {
        List<Entry> snapshot = new ArrayList<>(values.size());
        values.forEach((key, value) -> snapshot.add(new Entry(key.bytes(), value)));
        return List.copyOf(snapshot);
    }

    public record Entry(byte[] key, byte[] value) {
        public Entry {
            key = Objects.requireNonNull(key, "key").clone();
            value = Objects.requireNonNull(value, "value").clone();
        }

        @Override
        public byte[] key() {
            return key.clone();
        }

        @Override
        public byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(Object object) {
            return object instanceof Entry other
                    && Arrays.equals(key, other.key)
                    && Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(key) + Arrays.hashCode(value);
        }
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hashCode;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "key").clone();
            this.hashCode = Arrays.hashCode(this.bytes);
        }

        private byte[] bytes() {
            return bytes.clone();
        }

        @Override
        public boolean equals(Object object) {
            return this == object
                    || object instanceof ByteArrayKey other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
