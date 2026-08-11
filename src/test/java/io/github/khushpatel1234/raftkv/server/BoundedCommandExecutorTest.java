package io.github.khushpatel1234.raftkv.server;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedCommandExecutorTest {
    @Test
    void boundsTheQueueAndRejectsInsteadOfBlockingTheEventLoop() throws Exception {
        BoundedCommandExecutor executor = new BoundedCommandExecutor(1, 1);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch queuedRan = new CountDownLatch(1);
        try {
            executor.execute(() -> {
                started.countDown();
                await(release);
            });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            executor.execute(queuedRan::countDown);

            assertEquals(1, executor.activeCount());
            assertEquals(1, executor.queuedTaskCount());
            assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> { }));

            release.countDown();
            assertTrue(queuedRan.await(2, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            executor.shutdownGracefully(Duration.ofSeconds(2));
        }
        assertTrue(executor.isShutdown());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
