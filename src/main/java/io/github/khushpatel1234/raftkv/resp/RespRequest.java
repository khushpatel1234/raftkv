package io.github.khushpatel1234.raftkv.resp;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A binary-safe RESP command request. The first argument is the command name. */
public record RespRequest(List<byte[]> arguments) {
    public RespRequest {
        Objects.requireNonNull(arguments, "arguments");
        var copy = new ArrayList<byte[]>(arguments.size());
        for (byte[] argument : arguments) {
            copy.add(Objects.requireNonNull(argument, "argument").clone());
        }
        arguments = List.copyOf(copy);
    }

    @Override
    public List<byte[]> arguments() {
        var copy = new ArrayList<byte[]>(arguments.size());
        for (byte[] argument : arguments) {
            copy.add(argument.clone());
        }
        return List.copyOf(copy);
    }

    public int size() {
        return arguments.size();
    }

    public byte[] argument(int index) {
        return arguments.get(index).clone();
    }
}
