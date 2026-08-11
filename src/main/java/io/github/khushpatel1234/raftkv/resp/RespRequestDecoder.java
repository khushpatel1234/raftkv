package io.github.khushpatel1234.raftkv.resp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;

import java.util.ArrayList;
import java.util.List;

/**
 * Incrementally decodes the RESP2 array-of-bulk-strings form used for commands.
 * The state machine accepts fragmented and pipelined requests without reparsing bytes that have
 * already arrived, and enforces argument, bulk-string, and whole-frame limits before allocation.
 */
public final class RespRequestDecoder extends ByteToMessageDecoder {
    public static final int DEFAULT_MAX_ARGUMENTS = 1_024;
    public static final int DEFAULT_MAX_BULK_STRING_BYTES = 16 * 1024 * 1024;
    public static final int DEFAULT_MAX_FRAME_BYTES = 32 * 1024 * 1024;

    private static final int MAX_INTEGER_DIGITS = 10;

    private final int maxArguments;
    private final int maxBulkStringBytes;
    private final int maxFrameBytes;

    private State state = State.ARRAY_PREFIX;
    private int frameBytes;
    private int expectedArguments;
    private int bulkStringLength;
    private List<byte[]> arguments;

    public RespRequestDecoder() {
        this(DEFAULT_MAX_ARGUMENTS, DEFAULT_MAX_BULK_STRING_BYTES, DEFAULT_MAX_FRAME_BYTES);
    }

    public RespRequestDecoder(int maxArguments, int maxBulkStringBytes, int maxFrameBytes) {
        if (maxArguments < 1) {
            throw new IllegalArgumentException("maxArguments must be positive");
        }
        if (maxBulkStringBytes < 0) {
            throw new IllegalArgumentException("maxBulkStringBytes cannot be negative");
        }
        if (maxFrameBytes < 8) {
            throw new IllegalArgumentException("maxFrameBytes must be at least 8");
        }
        this.maxArguments = maxArguments;
        this.maxBulkStringBytes = maxBulkStringBytes;
        this.maxFrameBytes = maxFrameBytes;
    }

    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
        try {
            decodeCurrentState(input, output);
        } catch (DecoderException protocolFailure) {
            // The downstream protocol-error handler closes the connection. Discarding the
            // cumulation prevents channelInactive from trying to decode the invalid tail again.
            input.skipBytes(input.readableBytes());
            resetFrame();
            throw protocolFailure;
        }
    }

    private void decodeCurrentState(ByteBuf input, List<Object> output) {
        switch (state) {
            case ARRAY_PREFIX -> decodeArrayPrefix(input);
            case ARRAY_LENGTH -> decodeArrayLength(input, output);
            case BULK_PREFIX -> decodeBulkPrefix(input);
            case BULK_LENGTH -> decodeBulkLength(input);
            case BULK_DATA -> decodeBulkData(input, output);
        }
    }

    private void decodeArrayPrefix(ByteBuf input) {
        if (!input.isReadable()) {
            return;
        }
        addFrameBytes(1);
        if (input.readByte() != '*') {
            throw new CorruptedFrameException("RESP command must start with an array");
        }
        state = State.ARRAY_LENGTH;
    }

    private void decodeArrayLength(ByteBuf input, List<Object> output) {
        Line line = readIntegerLine(input);
        if (line == null) {
            return;
        }
        expectedArguments = parseNonNegativeInt(input, line, "array length");
        if (expectedArguments > maxArguments) {
            throw new TooLongFrameException(
                    "RESP command has more than " + maxArguments + " arguments");
        }

        arguments = new ArrayList<>(expectedArguments);
        if (expectedArguments == 0) {
            emitRequest(output);
        } else {
            state = State.BULK_PREFIX;
        }
    }

    private void decodeBulkPrefix(ByteBuf input) {
        if (!input.isReadable()) {
            return;
        }
        addFrameBytes(1);
        if (input.readByte() != '$') {
            throw new CorruptedFrameException("RESP command arguments must be bulk strings");
        }
        state = State.BULK_LENGTH;
    }

    private void decodeBulkLength(ByteBuf input) {
        Line line = readIntegerLine(input);
        if (line == null) {
            return;
        }
        bulkStringLength = parseNonNegativeInt(input, line, "bulk string length");
        if (bulkStringLength > maxBulkStringBytes) {
            throw new TooLongFrameException(
                    "RESP bulk string exceeds " + maxBulkStringBytes + " bytes");
        }
        state = State.BULK_DATA;
    }

    private void decodeBulkData(ByteBuf input, List<Object> output) {
        long requiredBytes = (long) bulkStringLength + 2L;
        if (input.readableBytes() < requiredBytes) {
            ensurePartialFrameIsWithinLimit(input);
            return;
        }
        addFrameBytes(requiredBytes);

        byte[] argument = new byte[bulkStringLength];
        input.readBytes(argument);
        if (input.readByte() != '\r' || input.readByte() != '\n') {
            throw new CorruptedFrameException("bulk string is not terminated by CRLF");
        }
        arguments.add(argument);

        if (arguments.size() == expectedArguments) {
            emitRequest(output);
        } else {
            state = State.BULK_PREFIX;
        }
    }

    private Line readIntegerLine(ByteBuf input) {
        int lineStart = input.readerIndex();
        int readableBytes = input.readableBytes();
        int scanLength = Math.min(readableBytes, MAX_INTEGER_DIGITS + 2);
        int scanEnd = lineStart + scanLength;
        for (int index = lineStart; index + 1 < scanEnd; index++) {
            if (input.getByte(index) == '\r' && input.getByte(index + 1) == '\n') {
                int consumedBytes = index + 2 - lineStart;
                input.skipBytes(consumedBytes);
                addFrameBytes(consumedBytes);
                return new Line(lineStart, index);
            }
        }

        ensurePartialFrameIsWithinLimit(input);
        if (readableBytes > MAX_INTEGER_DIGITS + 1) {
            throw new TooLongFrameException("RESP integer exceeds the supported integer range");
        }
        return null;
    }

    private static int parseNonNegativeInt(ByteBuf input, Line line, String description) {
        if (line.start == line.end) {
            throw new CorruptedFrameException(description + " is empty");
        }

        long value = 0L;
        for (int index = line.start; index < line.end; index++) {
            int digit = input.getUnsignedByte(index) - '0';
            if (digit < 0 || digit > 9) {
                throw new CorruptedFrameException(description + " is not a non-negative integer");
            }
            if (value > (Integer.MAX_VALUE - digit) / 10L) {
                throw new TooLongFrameException(description + " exceeds the supported integer range");
            }
            value = value * 10L + digit;
        }
        return (int) value;
    }

    private void emitRequest(List<Object> output) {
        output.add(new RespRequest(arguments));
        resetFrame();
    }

    private void ensurePartialFrameIsWithinLimit(ByteBuf input) {
        if ((long) frameBytes + input.readableBytes() > maxFrameBytes) {
            throw new TooLongFrameException("RESP command exceeds " + maxFrameBytes + " bytes");
        }
    }

    private void addFrameBytes(long bytes) {
        long newFrameBytes = frameBytes + bytes;
        if (newFrameBytes > maxFrameBytes) {
            throw new TooLongFrameException("RESP command exceeds " + maxFrameBytes + " bytes");
        }
        frameBytes = (int) newFrameBytes;
    }

    private void resetFrame() {
        state = State.ARRAY_PREFIX;
        frameBytes = 0;
        expectedArguments = 0;
        bulkStringLength = 0;
        arguments = null;
    }

    private enum State {
        ARRAY_PREFIX,
        ARRAY_LENGTH,
        BULK_PREFIX,
        BULK_LENGTH,
        BULK_DATA
    }

    private record Line(int start, int end) {
    }
}
