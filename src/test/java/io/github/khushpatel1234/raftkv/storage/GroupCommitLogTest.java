package io.github.khushpatel1234.raftkv.storage;

import io.github.khushpatel1234.raftkv.core.RaftCommand;
import io.github.khushpatel1234.raftkv.core.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupCommitLogTest {
    @TempDir
    Path tempDirectory;

    @Test
    void oneFutureCanSpanSeveralBoundedDurabilityBatches() throws IOException {
        Path path = tempDirectory.resolve("group.wal");
        List<RaftLogEntry> entries = entries(1, 10);
        CompletableFuture<Void> durable;
        try (GroupCommitLog log = new GroupCommitLog(path, 3, Duration.ZERO)) {
            durable = log.append(entries);
            durable.join();
            assertTrue(durable.isDone());
            assertEquals(10, log.lastIndex());
            GroupCommitMetrics metrics = log.metrics();
            assertEquals(4, metrics.batchCount());
            assertEquals(10, metrics.entryCount());
            assertEquals(3, metrics.maxBatchSize());
            assertEquals(2.5, metrics.averageBatchSize());
        }
        try (WriteAheadLog recovered = new WriteAheadLog(path)) {
            assertEquals(entries, recovered.recover());
        }
    }

    @Test
    void concurrentRequestsShareForcesAndExposeAccurateMetrics() throws IOException {
        Path path = tempDirectory.resolve("batched.wal");
        try (GroupCommitLog log = new GroupCommitLog(path, 64, Duration.ofMillis(30))) {
            List<CompletableFuture<Void>> futures = LongStream.rangeClosed(1, 20)
                    .mapToObj(index -> log.append(entry(index)))
                    .toList();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

            GroupCommitMetrics metrics = log.metrics();
            assertEquals(20, metrics.totalEntries());
            assertTrue(metrics.batchCount() < 20);
            assertTrue(metrics.maxBatchEntries() <= 64);
            assertEquals((double) metrics.entryCount() / metrics.batchCount(),
                    metrics.averageBatchEntries());
        }
    }

    @Test
    void truncationIsOrderedAndInvalidAppendDoesNotKillWriter() throws IOException {
        Path path = tempDirectory.resolve("truncate.wal");
        try (GroupCommitLog log = new GroupCommitLog(path, 8, Duration.ofMillis(1))) {
            assertThrows(CompletionException.class, () -> log.append(entry(2)).join());
            log.append(entries(1, 4)).join();
            log.truncateSuffix(3).join();
            assertEquals(2, log.lastIndex());
            log.append(entry(3)).join();
            assertEquals(List.of(entry(2), entry(3)), log.entriesFrom(2, 10));
        }
    }

    @Test
    void appendAfterCloseFails() throws IOException {
        GroupCommitLog log = new GroupCommitLog(tempDirectory.resolve("closed.wal"));
        log.close();
        var submission = log.submitAppend(entry(1));
        assertEquals(GroupCommitLog.SubmissionStatus.UNAVAILABLE, submission.status());
        CompletableFuture<Void> rejected = submission.durability();
        assertTrue(rejected.isCompletedExceptionally());
        assertFalse(rejected.isCancelled());
    }

    @Test
    void closeReturnsAfterTheWriterHasAlreadyFailed() throws IOException {
        var wal = new WriteAheadLog(tempDirectory.resolve("failed.wal"));
        var log = new GroupCommitLog(wal, 8, Duration.ZERO, 8);
        wal.close();

        assertThrows(CompletionException.class, () -> log.append(entry(1)).join());
        assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> assertThrows(IOException.class, log::close));
    }

    @Test
    void interruptedCloseCompletesTheSharedCloseFuture() throws IOException {
        var log = new GroupCommitLog(tempDirectory.resolve("interrupted-close.wal"));

        Thread.currentThread().interrupt();
        try {
            assertTimeout(Duration.ofSeconds(1),
                    () -> assertThrows(IOException.class, log::close));
        } finally {
            // close() restores the flag; never leak it into later JUnit tests.
            Thread.interrupted();
        }
        assertFalse(Thread.currentThread().isInterrupted());

        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
            try {
                log.close();
            } catch (IOException expected) {
                // An interrupted first close completes the shared future exceptionally.
            }
        });
    }

    private static List<RaftLogEntry> entries(long first, int count) {
        return LongStream.range(first, first + count).mapToObj(GroupCommitLogTest::entry).toList();
    }

    private static RaftLogEntry entry(long index) {
        return new RaftLogEntry(index, 1, RaftCommand.set(
                new byte[]{(byte) index}, new byte[]{(byte) index}));
    }
}
