package io.github.khushpatel1234.raftkv.server;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** A fixed-size command pool with a bounded queue and fail-fast saturation policy. */
public final class BoundedCommandExecutor implements Executor, AutoCloseable {
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final ThreadPoolExecutor executor;

    public BoundedCommandExecutor(int threadCount, int queueCapacity) {
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount must be positive");
        }
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }

        AtomicInteger threadSequence = new AtomicInteger();
        executor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                runnable -> {
                    Thread thread = new Thread(
                            runnable, "raftkv-command-" + threadSequence.incrementAndGet());
                    thread.setDaemon(false);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
        executor.prestartAllCoreThreads();
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(Objects.requireNonNull(command, "command"));
    }

    public int activeCount() {
        return executor.getActiveCount();
    }

    public int queuedTaskCount() {
        return executor.getQueue().size();
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public void close() {
        shutdownGracefully(DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public void shutdownGracefully(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout cannot be negative");
        }

        executor.shutdown();
        boolean terminated = false;
        try {
            terminated = executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        if (!terminated) {
            executor.shutdownNow();
        }
    }
}
