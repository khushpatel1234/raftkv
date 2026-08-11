package io.github.khushpatel1234.raftkv.resp;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Values that can be encoded as RESP2 responses. */
public sealed interface RespResponse permits RespResponse.SimpleString, RespResponse.Error,
        RespResponse.IntegerValue, RespResponse.BulkString, RespResponse.ArrayValue {

    static RespResponse simple(String value) {
        return new SimpleString(value);
    }

    static RespResponse error(String message) {
        return new Error(message);
    }

    static RespResponse integer(long value) {
        return new IntegerValue(value);
    }

    static RespResponse bulk(byte[] value) {
        return new BulkString(Objects.requireNonNull(value, "value"));
    }

    static RespResponse bulk(String value) {
        return bulk(Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8));
    }

    static RespResponse nullBulk() {
        return new BulkString(null);
    }

    static RespResponse array(List<? extends RespResponse> values) {
        return new ArrayValue(List.copyOf(values));
    }

    record SimpleString(String value) implements RespResponse {
        public SimpleString {
            requireLineSafe(value, "simple string");
        }
    }

    record Error(String message) implements RespResponse {
        public Error {
            requireLineSafe(message, "error");
        }
    }

    record IntegerValue(long value) implements RespResponse {
    }

    /** A {@code null} value represents the RESP2 null bulk string. */
    record BulkString(byte[] value) implements RespResponse {
        public BulkString {
            value = value == null ? null : value.clone();
        }

        @Override
        public byte[] value() {
            return value == null ? null : value.clone();
        }
    }

    record ArrayValue(List<RespResponse> values) implements RespResponse {
        public ArrayValue {
            values = List.copyOf(values);
        }
    }

    private static void requireLineSafe(String value, String type) {
        Objects.requireNonNull(value, type);
        if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(type + " cannot contain CR or LF");
        }
    }
}
