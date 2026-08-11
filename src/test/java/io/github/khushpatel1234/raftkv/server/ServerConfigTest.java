package io.github.khushpatel1234.raftkv.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerConfigTest {
    @Test
    void productionDefaultsUseSixteenCommandThreadsAndExplicitWatermarks() {
        ServerConfig config = ServerConfig.localhost(6379);
        assertEquals(16, config.commandThreads());
        assertEquals(256, config.pendingResponseLowWatermark());
        assertEquals(512, config.pendingResponseHighWatermark());
        assertEquals(32 * 1024, config.writeBufferLowWatermarkBytes());
        assertEquals(64 * 1024, config.writeBufferHighWatermarkBytes());
    }

    @Test
    void rejectsInvalidWatermarks() {
        ServerConfig defaults = ServerConfig.localhost(6379);
        assertThrows(IllegalArgumentException.class, () -> new ServerConfig(
                defaults.host(),
                defaults.port(),
                defaults.commandThreads(),
                defaults.commandQueueCapacity(),
                10,
                10,
                defaults.writeBufferLowWatermarkBytes(),
                defaults.writeBufferHighWatermarkBytes(),
                defaults.maxArguments(),
                defaults.maxBulkStringBytes(),
                defaults.maxFrameBytes()));
    }
}
