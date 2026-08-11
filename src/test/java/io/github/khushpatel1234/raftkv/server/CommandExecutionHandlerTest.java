package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespRequest;
import io.github.khushpatel1234.raftkv.resp.RespRequestDecoder;
import io.github.khushpatel1234.raftkv.resp.RespResponse;
import io.github.khushpatel1234.raftkv.resp.RespResponseEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandExecutionHandlerTest {
    @Test
    void writesAsyncResponsesInRequestOrder() {
        List<CompletableFuture<RespResponse>> responses = new ArrayList<>();
        CommandDispatcher dispatcher = command -> {
            CompletableFuture<RespResponse> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        };
        CommandExecutionHandler handler = handler(dispatcher, Runnable::run, 1, 4);
        EmbeddedChannel channel = channel(handler);
        try {
            channel.writeInbound(request("GET", "first"), request("GET", "second"));
            assertEquals(2, handler.pendingResponseCount());

            responses.get(1).complete(RespResponse.bulk("second"));
            channel.runPendingTasks();
            assertNull(channel.readOutbound());

            responses.get(0).complete(RespResponse.bulk("first"));
            channel.runPendingTasks();
            assertEquals("$5\r\nfirst\r\n", readAscii(channel));
            assertEquals("$6\r\nsecond\r\n", readAscii(channel));
            assertEquals(0, handler.pendingResponseCount());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void commandSyntaxErrorsRemainOrderedBehindEarlierWork() {
        CompletableFuture<RespResponse> firstResponse = new CompletableFuture<>();
        CommandExecutionHandler handler = handler(command -> firstResponse, Runnable::run, 1, 4);
        EmbeddedChannel channel = channel(handler);
        try {
            channel.writeInbound(request("GET", "key"), request("GET"));
            assertNull(channel.readOutbound());

            firstResponse.complete(RespResponse.nullBulk());
            channel.runPendingTasks();
            assertEquals("$-1\r\n", readAscii(channel));
            assertEquals("-ERR wrong number of arguments for 'get' command\r\n", readAscii(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void pingAndEchoAreLocalAndBinarySafe() {
        AtomicInteger dispatchCount = new AtomicInteger();
        CommandDispatcher dispatcher = command -> {
            dispatchCount.incrementAndGet();
            return CompletableFuture.completedFuture(RespResponse.error("ERR unexpected"));
        };
        EmbeddedChannel channel = channel(handler(dispatcher, Runnable::run, 1, 4));
        try {
            channel.writeInbound(request("PING"), request("PING", "hi"), request("ECHO", "hello"));
            channel.runPendingTasks();

            assertEquals("+PONG\r\n", readAscii(channel));
            assertEquals("$2\r\nhi\r\n", readAscii(channel));
            assertEquals("$5\r\nhello\r\n", readAscii(channel));
            assertEquals(0, dispatchCount.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void pausesAndResumesAutoReadAtPendingWatermarks() {
        List<CompletableFuture<RespResponse>> responses = new ArrayList<>();
        CommandDispatcher dispatcher = command -> {
            CompletableFuture<RespResponse> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        };
        CommandExecutionHandler handler = handler(dispatcher, Runnable::run, 1, 2);
        EmbeddedChannel channel = channel(handler);
        try {
            assertTrue(channel.config().isAutoRead());
            channel.writeInbound(request("GET", "one"), request("GET", "two"));
            assertFalse(channel.config().isAutoRead());

            responses.get(0).complete(RespResponse.nullBulk());
            channel.runPendingTasks();
            assertTrue(channel.config().isAutoRead());
            assertEquals(1, handler.pendingResponseCount());

            responses.get(1).complete(RespResponse.nullBulk());
            channel.runPendingTasks();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void returnsBusyWhenTheBoundedExecutorRejectsWork() {
        CommandExecutionHandler handler = handler(
                command -> CompletableFuture.completedFuture(RespResponse.simple("OK")),
                command -> {
                    throw new RejectedExecutionException("full");
                },
                1,
                4);
        EmbeddedChannel channel = channel(handler);
        try {
            channel.writeInbound(request("GET", "key"));
            assertEquals("-BUSY command queue is full\r\n", readAscii(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void hidesUnexpectedDispatcherFailuresFromClients() {
        CommandDispatcher dispatcher = command ->
                CompletableFuture.failedFuture(new IllegalStateException("secret detail"));
        EmbeddedChannel channel = channel(handler(dispatcher, Runnable::run, 1, 4));
        try {
            channel.writeInbound(request("GET", "key"));
            channel.runPendingTasks();
            assertEquals("-ERR internal server error\r\n", readAscii(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void turnsDecoderFailuresIntoProtocolErrorsAndClosesConnection() {
        CommandDispatcher dispatcher = command ->
                CompletableFuture.completedFuture(RespResponse.simple("OK"));
        EmbeddedChannel channel = new EmbeddedChannel(
                new RespRequestDecoder(),
                new RespResponseEncoder(),
                handler(dispatcher, Runnable::run, 1, 4));
        try {
            channel.writeInbound(Unpooled.copiedBuffer("+PING\r\n", StandardCharsets.US_ASCII));
            channel.runPendingTasks();
            assertEquals("-ERR Protocol error\r\n", readAscii(channel));
            assertFalse(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static CommandExecutionHandler handler(
            CommandDispatcher dispatcher,
            java.util.concurrent.Executor executor,
            int lowWatermark,
            int highWatermark) {
        return new CommandExecutionHandler(dispatcher, executor, lowWatermark, highWatermark);
    }

    private static EmbeddedChannel channel(CommandExecutionHandler handler) {
        return new EmbeddedChannel(new RespResponseEncoder(), handler);
    }

    private static RespRequest request(String... arguments) {
        return new RespRequest(Arrays.stream(arguments)
                .map(argument -> argument.getBytes(StandardCharsets.US_ASCII))
                .toList());
    }

    private static String readAscii(EmbeddedChannel channel) {
        ByteBuf response = channel.readOutbound();
        try {
            return response.toString(StandardCharsets.US_ASCII);
        } finally {
            response.release();
        }
    }
}
