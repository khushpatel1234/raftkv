package io.github.khushpatel1234.raftkv.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Typed, binary-safe commands accepted by the RESP server. */
public sealed interface Command permits Command.Ping, Command.Echo, Command.Get, Command.Set,
        Command.Del, Command.Info, Command.GroupStats {

    record Ping(Optional<byte[]> message) implements Command {
        public Ping {
            Objects.requireNonNull(message, "message");
            message = message.map(Command::copy);
        }

        public Ping() {
            this(Optional.empty());
        }

        public Ping(byte[] message) {
            this(Optional.of(Objects.requireNonNull(message, "message")));
        }

        @Override
        public Optional<byte[]> message() {
            return message.map(Command::copy);
        }
    }

    record Echo(byte[] message) implements Command {
        public Echo {
            message = copy(message);
        }

        @Override
        public byte[] message() {
            return copy(message);
        }
    }

    record Get(byte[] key) implements Command {
        public Get {
            key = copy(key);
        }

        @Override
        public byte[] key() {
            return copy(key);
        }
    }

    record Set(byte[] key, byte[] value) implements Command {
        public Set {
            key = copy(key);
            value = copy(value);
        }

        @Override
        public byte[] key() {
            return copy(key);
        }

        @Override
        public byte[] value() {
            return copy(value);
        }
    }

    record Del(List<byte[]> keys) implements Command {
        public Del {
            Objects.requireNonNull(keys, "keys");
            List<byte[]> copiedKeys = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                copiedKeys.add(copy(key));
            }
            keys = List.copyOf(copiedKeys);
        }

        @Override
        public List<byte[]> keys() {
            List<byte[]> copiedKeys = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                copiedKeys.add(copy(key));
            }
            return List.copyOf(copiedKeys);
        }
    }

    record Info() implements Command {
    }

    record GroupStats() implements Command {
    }

    private static byte[] copy(byte[] value) {
        return Objects.requireNonNull(value, "value").clone();
    }
}
