package io.github.khushpatel1234.raftkv.storage;

import io.github.khushpatel1234.raftkv.core.RaftLogEntry;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous WAL writer that amortizes one {@code force(false)} over a batch.
 * Append futures are completed only after the force for every entry represented
 * by that future succeeds.
 */
public final class GroupCommitLog implements AutoCloseable {
    public static final int DEFAULT_MAX_BATCH_SIZE = 64;
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofMillis(2);
    public static final int DEFAULT_MAX_PENDING_REQUESTS = 8_192;

    private final WriteAheadLog wal;
    private final int maxBatchSize;
    private final long maxDelayNanos;
    private final ArrayBlockingQueue<Request> queue;
    private final Object submissionLock = new Object();
    private final Thread writerThread;
    private final AtomicLong batchCount = new AtomicLong();
    private final AtomicLong entryCount = new AtomicLong();
    private final AtomicInteger observedMaxBatchSize = new AtomicInteger();

    private volatile Throwable terminalFailure;
    private boolean accepting = true;
    private CompletableFuture<Void> closeFuture;

    public GroupCommitLog(java.nio.file.Path path) throws IOException {
        this(path, DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_DELAY);
    }

    public GroupCommitLog(java.nio.file.Path path, int maxBatchSize, Duration maxDelay)
            throws IOException {
        this(new WriteAheadLog(path), maxBatchSize, maxDelay, DEFAULT_MAX_PENDING_REQUESTS);
    }

    public GroupCommitLog(
            WriteAheadLog wal,
            int maxBatchSize,
            Duration maxDelay,
            int maxPendingRequests) {
        this.wal = Objects.requireNonNull(wal, "wal");
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        Objects.requireNonNull(maxDelay, "maxDelay");
        if (maxDelay.isNegative()) {
            throw new IllegalArgumentException("maxDelay cannot be negative");
        }
        if (maxPendingRequests < 1) {
            throw new IllegalArgumentException("maxPendingRequests must be positive");
        }
        this.maxBatchSize = maxBatchSize;
        try {
            maxDelayNanos = maxDelay.toNanos();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("maxDelay is too large", exception);
        }
        queue = new ArrayBlockingQueue<>(maxPendingRequests);
        writerThread = new Thread(this::writerLoop, "raftkv-group-commit");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    public CompletableFuture<Void> append(RaftLogEntry entry) {
        return append(List.of(Objects.requireNonNull(entry, "entry")));
    }

    /** Appends a contiguous list; the returned future covers every entry. */
    public CompletableFuture<Void> append(List<RaftLogEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        if (entries.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<RaftLogEntry> copied;
        try {
            copied = List.copyOf(entries);
        } catch (NullPointerException exception) {
            return CompletableFuture.failedFuture(exception);
        }
        AppendRequest request = new AppendRequest(copied);
        return submit(request);
    }

    /** Serializes a durable suffix truncation after all earlier append requests. */
    public CompletableFuture<Void> truncateSuffix(long fromIndexInclusive) {
        TruncateRequest request = new TruncateRequest(fromIndexInclusive);
        return submit(request);
    }

    public List<RaftLogEntry> recover() {
        return wal.recover();
    }

    public List<RaftLogEntry> entries() {
        return wal.entries();
    }

    public Optional<RaftLogEntry> entry(long index) {
        return wal.entry(index);
    }

    public List<RaftLogEntry> entriesFrom(long fromIndexInclusive, int maxEntries) {
        return wal.entriesFrom(fromIndexInclusive, maxEntries);
    }

    public long lastIndex() {
        return wal.lastIndex();
    }

    public WriteAheadLog.RecoveryReport recoveryReport() {
        return wal.recoveryReport();
    }

    public GroupCommitMetrics metrics() {
        long batches = batchCount.get();
        long durableEntries = entryCount.get();
        double average = batches == 0 ? 0.0 : (double) durableEntries / batches;
        return new GroupCommitMetrics(
                batches,
                durableEntries,
                observedMaxBatchSize.get(),
                average);
    }

    public int pendingRequests() {
        return queue.size();
    }

    @Override
    public void close() throws IOException {
        CompletableFuture<Void> completion;
        CloseRequest closeRequest = null;
        synchronized (submissionLock) {
            if (closeFuture == null) {
                accepting = false;
                closeFuture = new CompletableFuture<>();
                closeRequest = new CloseRequest(closeFuture);
            }
            completion = closeFuture;
        }
        if (closeRequest != null) {
            try {
                queue.put(closeRequest);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while closing group commit log", exception);
            }
        }
        try {
            completion.join();
        } catch (CompletionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("Could not close group commit log", cause);
        }
    }

    private CompletableFuture<Void> submit(Request request) {
        synchronized (submissionLock) {
            if (!accepting) {
                Throwable failure = terminalFailure;
                if (failure == null) {
                    failure = new IllegalStateException("Group commit log is closed");
                }
                request.fail(failure);
                return request.future();
            }
            if (!queue.offer(request)) {
                request.fail(new RejectedExecutionException("Group commit queue is full"));
            }
            return request.future();
        }
    }

    private void writerLoop() {
        Request deferred = null;
        try {
            while (true) {
                Request request;
                if (deferred != null) {
                    request = deferred;
                    deferred = null;
                } else {
                    request = queue.take();
                }

                if (request instanceof AppendRequest appendRequest) {
                    deferred = writeBatch(appendRequest);
                } else if (request instanceof TruncateRequest truncateRequest) {
                    handleTruncate(truncateRequest);
                } else if (request instanceof CloseRequest closeRequest) {
                    try {
                        wal.close();
                        closeRequest.succeed();
                    } catch (IOException exception) {
                        closeRequest.fail(exception);
                        throw exception;
                    }
                    return;
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            failTerminal(new IOException("Group commit writer was interrupted", exception), deferred);
        } catch (IOException exception) {
            failTerminal(exception, deferred);
        } catch (RuntimeException exception) {
            failTerminal(exception, deferred);
        }
    }

    /** Returns a request that was dequeued but belongs after the current batch. */
    private Request writeBatch(AppendRequest first) throws IOException, InterruptedException {
        List<RaftLogEntry> batch = new ArrayList<>(maxBatchSize);
        Map<AppendRequest, Integer> durableCounts = new LinkedHashMap<>();
        AppendRequest current = first;
        Request deferred = null;
        long deadline = System.nanoTime() + maxDelayNanos;
        long expectedIndex = wal.lastIndex() + 1;

        while (batch.size() < maxBatchSize) {
            if (current != null) {
                if (!current.validated) {
                    try {
                        validateSequence(current.entries, expectedIndex);
                        current.validated = true;
                    } catch (IllegalArgumentException exception) {
                        current.fail(exception);
                        current = null;
                        continue;
                    }
                }

                int count = Math.min(maxBatchSize - batch.size(), current.remaining());
                for (int offset = 0; offset < count; offset++) {
                    batch.add(current.entries.get(current.cursor + offset));
                }
                current.cursor += count;
                expectedIndex += count;
                durableCounts.merge(current, count, Integer::sum);
                if (current.remaining() > 0) {
                    deferred = current;
                    break;
                }
                current = null;
                if (batch.size() == maxBatchSize) {
                    break;
                }
            }

            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            Request next = queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
            if (next == null) {
                break;
            }
            if (next instanceof AppendRequest nextAppend) {
                current = nextAppend;
            } else {
                deferred = next;
                break;
            }
        }

        if (batch.isEmpty()) {
            return deferred;
        }

        try {
            wal.appendUnforced(batch);
            wal.force();
        } catch (IllegalArgumentException exception) {
            for (AppendRequest request : durableCounts.keySet()) {
                request.fail(exception);
            }
            if (deferred instanceof AppendRequest appendRequest && appendRequest.failed) {
                deferred = null;
            }
            return deferred;
        } catch (IOException exception) {
            for (AppendRequest request : durableCounts.keySet()) {
                request.fail(exception);
            }
            if (deferred != null) {
                deferred.fail(exception);
            }
            throw exception;
        } catch (RuntimeException exception) {
            for (AppendRequest request : durableCounts.keySet()) {
                request.fail(exception);
            }
            if (deferred != null) {
                deferred.fail(exception);
            }
            throw exception;
        }

        int size = batch.size();
        entryCount.addAndGet(size);
        batchCount.incrementAndGet();
        observedMaxBatchSize.accumulateAndGet(size, Math::max);
        durableCounts.forEach(AppendRequest::durable);
        return deferred;
    }

    private void handleTruncate(TruncateRequest request) throws IOException {
        try {
            wal.truncateSuffix(request.fromIndexInclusive);
            request.succeed();
        } catch (IllegalArgumentException exception) {
            request.fail(exception);
        }
    }

    private void failTerminal(Throwable failure, Request deferred) {
        terminalFailure = failure;
        synchronized (submissionLock) {
            accepting = false;
        }
        if (deferred != null) {
            deferred.fail(failure);
        }
        Request request;
        while ((request = queue.poll()) != null) {
            request.fail(failure);
        }
        try {
            wal.close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static void validateSequence(List<RaftLogEntry> entries, long expectedIndex) {
        long expected = expectedIndex;
        for (RaftLogEntry entry : entries) {
            if (entry.index() != expected) {
                throw new IllegalArgumentException(
                        "Expected log index " + expected + " but received " + entry.index());
            }
            expected++;
        }
    }

    private sealed interface Request permits AppendRequest, TruncateRequest, CloseRequest {
        CompletableFuture<Void> future();

        default void succeed() {
            future().complete(null);
        }

        default void fail(Throwable failure) {
            future().completeExceptionally(failure);
        }
    }

    private static final class AppendRequest implements Request {
        private final List<RaftLogEntry> entries;
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private int cursor;
        private int pendingDurability;
        private boolean validated;
        private boolean failed;

        private AppendRequest(List<RaftLogEntry> entries) {
            this.entries = entries;
            pendingDurability = entries.size();
        }

        private int remaining() {
            return entries.size() - cursor;
        }

        private void durable(int count) {
            pendingDurability -= count;
            if (pendingDurability == 0) {
                succeed();
            }
        }

        @Override
        public void fail(Throwable failure) {
            failed = true;
            Request.super.fail(failure);
        }

        @Override
        public CompletableFuture<Void> future() {
            return future;
        }
    }

    private static final class TruncateRequest implements Request {
        private final long fromIndexInclusive;
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        private TruncateRequest(long fromIndexInclusive) {
            this.fromIndexInclusive = fromIndexInclusive;
        }

        @Override
        public CompletableFuture<Void> future() {
            return future;
        }
    }

    private static final class CloseRequest implements Request {
        private final CompletableFuture<Void> future;

        private CloseRequest(CompletableFuture<Void> future) {
            this.future = future;
        }

        @Override
        public CompletableFuture<Void> future() {
            return future;
        }
    }
}
