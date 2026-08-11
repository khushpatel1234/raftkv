package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespResponse;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftKvServerTest {
    @Test
    void startsOnAnEphemeralPortUsesCommandPoolAndClosesGracefully() throws Exception {
        AtomicReference<String> dispatchThread = new AtomicReference<>();
        CommandDispatcher dispatcher = command -> {
            dispatchThread.set(Thread.currentThread().getName());
            return CompletableFuture.completedFuture(RespResponse.nullBulk());
        };
        ServerConfig config = smallTestConfig();
        RaftKvServer server = new RaftKvServer(config, dispatcher);
        Socket client = new Socket();
        try {
            InetSocketAddress address = server.start();
            assertTrue(server.isRunning());
            assertTrue(address.getPort() > 0);

            client.connect(address);
            client.setSoTimeout(2_000);
            client.getOutputStream().write((
                    "*1\r\n$4\r\nPING\r\n"
                            + "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            client.getOutputStream().flush();

            InputStream input = client.getInputStream();
            assertEquals("+PONG\r\n", new String(input.readNBytes(7), StandardCharsets.US_ASCII));
            assertEquals("$-1\r\n", new String(input.readNBytes(5), StandardCharsets.US_ASCII));
            assertTrue(dispatchThread.get().startsWith("raftkv-command-"));

            server.close();
            assertFalse(server.isRunning());
            assertEquals(-1, input.read());
            server.close();
        } finally {
            client.close();
            server.close();
        }
        assertFalse(server.isRunning());
    }

    private static ServerConfig smallTestConfig() {
        return new ServerConfig(
                "127.0.0.1",
                0,
                1,
                16,
                2,
                4,
                1_024,
                2_048,
                16,
                1_024,
                2_048);
    }
}
