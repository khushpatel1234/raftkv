package io.github.khushpatel1234.raftkv.resp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RespResponseEncoderTest {
    @Test
    void encodesAllResponseKinds() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespResponseEncoder());
        try {
            channel.writeOutbound(RespResponse.simple("OK"));
            assertEquals("+OK\r\n", readAscii(channel));

            channel.writeOutbound(RespResponse.error("ERR nope"));
            assertEquals("-ERR nope\r\n", readAscii(channel));

            channel.writeOutbound(RespResponse.integer(-12));
            assertEquals(":-12\r\n", readAscii(channel));

            channel.writeOutbound(RespResponse.nullBulk());
            assertEquals("$-1\r\n", readAscii(channel));

            channel.writeOutbound(RespResponse.bulk("hello"));
            assertEquals("$5\r\nhello\r\n", readAscii(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void bulkStringsAreBinarySafe() {
        byte[] value = new byte[] {0, '\r', '\n', (byte) 0xff};
        EmbeddedChannel channel = new EmbeddedChannel(new RespResponseEncoder());
        try {
            channel.writeOutbound(RespResponse.bulk(value));
            ByteBuf encoded = channel.readOutbound();
            try {
                byte[] actual = new byte[encoded.readableBytes()];
                encoded.readBytes(actual);
                assertArrayEquals(
                        new byte[] {'$', '4', '\r', '\n', 0, '\r', '\n', (byte) 0xff, '\r', '\n'},
                        actual);
            } finally {
                encoded.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void encodesNestedArrays() {
        RespResponse response = RespResponse.array(List.of(
                RespResponse.simple("stats"),
                RespResponse.array(List.of(RespResponse.integer(2), RespResponse.nullBulk()))));
        EmbeddedChannel channel = new EmbeddedChannel(new RespResponseEncoder());
        try {
            channel.writeOutbound(response);
            assertEquals("*2\r\n+stats\r\n*2\r\n:2\r\n$-1\r\n", readAscii(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsLineInjectionInSimpleStringsAndErrors() {
        assertThrows(IllegalArgumentException.class, () -> RespResponse.simple("OK\r\n+forged"));
        assertThrows(IllegalArgumentException.class, () -> RespResponse.error("ERR bad\nline"));
    }

    private static String readAscii(EmbeddedChannel channel) {
        ByteBuf encoded = channel.readOutbound();
        try {
            return encoded.toString(StandardCharsets.US_ASCII);
        } finally {
            encoded.release();
        }
    }
}
