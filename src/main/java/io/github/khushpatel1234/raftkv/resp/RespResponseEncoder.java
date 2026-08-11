package io.github.khushpatel1234.raftkv.resp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/** Encodes {@link RespResponse} values using RESP2. */
public final class RespResponseEncoder extends MessageToByteEncoder<RespResponse> {
    private static final int MAX_ARRAY_NESTING = 64;

    @Override
    protected void encode(ChannelHandlerContext context, RespResponse response, ByteBuf output) {
        writeResponse(response, output, 0);
    }

    private static void writeResponse(RespResponse response, ByteBuf output, int depth) {
        if (depth > MAX_ARRAY_NESTING) {
            throw new EncoderException("RESP array nesting exceeds " + MAX_ARRAY_NESTING);
        }

        switch (response) {
            case RespResponse.SimpleString simple -> writeLine('+', simple.value(), output);
            case RespResponse.Error error -> writeLine('-', error.message(), output);
            case RespResponse.IntegerValue integer -> writeLine(':', Long.toString(integer.value()), output);
            case RespResponse.BulkString bulk -> writeBulkString(bulk.value(), output);
            case RespResponse.ArrayValue array -> {
                writeLine('*', Integer.toString(array.values().size()), output);
                for (RespResponse value : array.values()) {
                    writeResponse(value, output, depth + 1);
                }
            }
        }
    }

    private static void writeBulkString(byte[] value, ByteBuf output) {
        if (value == null) {
            writeLine('$', "-1", output);
            return;
        }
        writeLine('$', Integer.toString(value.length), output);
        output.writeBytes(value);
        writeCrlf(output);
    }

    private static void writeLine(char prefix, String value, ByteBuf output) {
        output.writeByte(prefix);
        output.writeCharSequence(value, StandardCharsets.UTF_8);
        writeCrlf(output);
    }

    private static void writeCrlf(ByteBuf output) {
        output.writeByte('\r');
        output.writeByte('\n');
    }
}
