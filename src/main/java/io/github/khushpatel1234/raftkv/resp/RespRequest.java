package io.github.khushpatel1234.raftkv.resp;

import java.util.List;
import java.util.Objects;

/** A binary-safe RESP command request. The first argument is the command name. */
public record RespRequest(List<byte[]> arguments) {
    public RespRequest {
        Objects.requireNonNull(arguments, "arguments");
        arguments = List.copyOf(arguments);
        for (byte[] argument : arguments) {
            Objects.requireNonNull(argument, "argument");
        }
    }

    public int size() {
        return arguments.size();
    }

    public byte[] argument(int index) {
        return arguments.get(index);
    }
}
