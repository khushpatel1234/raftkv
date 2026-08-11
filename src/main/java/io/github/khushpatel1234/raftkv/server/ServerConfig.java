package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespRequestDecoder;

/** Network, protocol-safety, and backpressure settings for {@link RaftKvServer}. */
public record ServerConfig(
        String host,
        int port,
        int commandThreads,
        int commandQueueCapacity,
        int pendingResponseLowWatermark,
        int pendingResponseHighWatermark,
        int writeBufferLowWatermarkBytes,
        int writeBufferHighWatermarkBytes,
        int maxArguments,
        int maxBulkStringBytes,
        int maxFrameBytes) {

    public static final int DEFAULT_COMMAND_THREADS = 16;

    public ServerConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host cannot be blank");
        }
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (commandThreads < 1) {
            throw new IllegalArgumentException("commandThreads must be positive");
        }
        if (commandQueueCapacity < 1) {
            throw new IllegalArgumentException("commandQueueCapacity must be positive");
        }
        if (pendingResponseLowWatermark < 0
                || pendingResponseHighWatermark <= pendingResponseLowWatermark) {
            throw new IllegalArgumentException("pending response watermarks must satisfy 0 <= low < high");
        }
        if (writeBufferLowWatermarkBytes < 0
                || writeBufferHighWatermarkBytes < writeBufferLowWatermarkBytes) {
            throw new IllegalArgumentException("write buffer watermarks must satisfy 0 <= low <= high");
        }
        if (maxArguments < 1 || maxBulkStringBytes < 0 || maxFrameBytes < 8) {
            throw new IllegalArgumentException("invalid RESP safety limits");
        }
    }

    public static ServerConfig defaults(String host, int port) {
        return new ServerConfig(
                host,
                port,
                DEFAULT_COMMAND_THREADS,
                8_192,
                256,
                512,
                32 * 1024,
                64 * 1024,
                RespRequestDecoder.DEFAULT_MAX_ARGUMENTS,
                RespRequestDecoder.DEFAULT_MAX_BULK_STRING_BYTES,
                RespRequestDecoder.DEFAULT_MAX_FRAME_BYTES);
    }

    public static ServerConfig localhost(int port) {
        return defaults("127.0.0.1", port);
    }
}
