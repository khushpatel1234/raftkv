package io.github.khushpatel1234.raftkv.resp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RespRequestDecoderTest {
    @Test
    void decodesAFrameFragmentedAtEveryByteBoundary() {
        byte[] frame = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
                .getBytes(StandardCharsets.US_ASCII);
        EmbeddedChannel channel = new EmbeddedChannel(new RespRequestDecoder());
        try {
            for (int index = 0; index < frame.length; index++) {
                boolean produced = channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {frame[index]}));
                assertEquals(index == frame.length - 1, produced);
            }

            RespRequest request = channel.readInbound();
            assertEquals(3, request.size());
            assertArrayEquals(bytes("SET"), request.argument(0));
            assertArrayEquals(bytes("key"), request.argument(1));
            assertArrayEquals(bytes("value"), request.argument(2));
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void decodesPipelinedFramesAndPreservesBinaryArguments() {
        byte[] binary = new byte[] {0, '\r', '\n', (byte) 0xff};
        ByteBuf input = Unpooled.buffer();
        input.writeCharSequence("*2\r\n$4\r\nECHO\r\n$4\r\n", StandardCharsets.US_ASCII);
        input.writeBytes(binary);
        input.writeCharSequence("\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", StandardCharsets.US_ASCII);

        EmbeddedChannel channel = new EmbeddedChannel(new RespRequestDecoder());
        try {
            assertTrue(channel.writeInbound(input));
            RespRequest first = channel.readInbound();
            RespRequest second = channel.readInbound();

            assertArrayEquals(bytes("ECHO"), first.argument(0));
            assertArrayEquals(binary, first.argument(1));
            assertArrayEquals(bytes("GET"), second.argument(0));
            assertArrayEquals(bytes("k"), second.argument(1));
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void waitsForTheBulkPayloadAndItsTrailingCrlf() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespRequestDecoder());
        try {
            assertFalse(channel.writeInbound(buffer("*1\r\n$4\r\nPING")));
            assertNull(channel.readInbound());
            assertFalse(channel.writeInbound(buffer("\r")));
            assertTrue(channel.writeInbound(buffer("\n")));
            RespRequest request = channel.readInbound();
            assertArrayEquals(bytes("PING"), request.argument(0));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsTooManyArguments() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespRequestDecoder(2, 32, 128));
        try {
            assertThrows(TooLongFrameException.class,
                    () -> channel.writeInbound(buffer("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsOversizedBulkStringsBeforeAllocatingThem() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespRequestDecoder(4, 3, 128));
        try {
            assertThrows(TooLongFrameException.class,
                    () -> channel.writeInbound(buffer("*1\r\n$4\r\n")));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsAnOversizedIncompleteFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespRequestDecoder(4, 64, 16));
        try {
            assertThrows(TooLongFrameException.class,
                    () -> channel.writeInbound(buffer("*1\r\n$12\r\n12345678")));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void appliesFrameLimitToEachPipelinedFrameNotTheWholeRead() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespRequestDecoder(2, 8, 20));
        try {
            assertTrue(channel.writeInbound(buffer("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")));
            assertEquals(1, ((RespRequest) channel.readInbound()).size());
            assertEquals(1, ((RespRequest) channel.readInbound()).size());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsNonArrayAndNonBulkRequestShapes() {
        EmbeddedChannel nonArray = new EmbeddedChannel(new RespRequestDecoder());
        EmbeddedChannel nonBulk = new EmbeddedChannel(new RespRequestDecoder());
        try {
            assertThrows(CorruptedFrameException.class,
                    () -> nonArray.writeInbound(buffer("+PING\r\n")));
            assertThrows(CorruptedFrameException.class,
                    () -> nonBulk.writeInbound(buffer("*1\r\n+PING\r\n")));
        } finally {
            nonArray.finishAndReleaseAll();
            nonBulk.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsNullAndNonNumericBulkLengths() {
        EmbeddedChannel nullBulk = new EmbeddedChannel(new RespRequestDecoder());
        EmbeddedChannel nonNumeric = new EmbeddedChannel(new RespRequestDecoder());
        try {
            assertThrows(CorruptedFrameException.class,
                    () -> nullBulk.writeInbound(buffer("*1\r\n$-1\r\n")));
            assertThrows(CorruptedFrameException.class,
                    () -> nonNumeric.writeInbound(buffer("*1\r\n$wat\r\n")));
        } finally {
            nullBulk.finishAndReleaseAll();
            nonNumeric.finishAndReleaseAll();
        }
    }

    private static ByteBuf buffer(String value) {
        return Unpooled.copiedBuffer(value, StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
