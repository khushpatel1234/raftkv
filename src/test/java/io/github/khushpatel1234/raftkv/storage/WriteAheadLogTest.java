package io.github.khushpatel1234.raftkv.storage;

import io.github.khushpatel1234.raftkv.core.RaftCommand;
import io.github.khushpatel1234.raftkv.core.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteAheadLogTest {
    @TempDir
    Path tempDirectory;

    @Test
    void appendsRecoversReadsAndDurablyTruncatesSuffix() throws IOException {
        Path path = tempDirectory.resolve("raft.wal");
        try (WriteAheadLog wal = new WriteAheadLog(path)) {
            wal.appendUnforced(entries(1, 5));
            wal.force();
            assertEquals(5, wal.lastIndex());
            assertEquals(List.of(entries(3, 2).getFirst(), entries(4, 1).getFirst()),
                    wal.entriesFrom(3, 2));
            wal.truncateSuffix(4);
            assertEquals(3, wal.lastIndex());
            assertTrue(wal.entry(4).isEmpty());
        }

        try (WriteAheadLog recovered = new WriteAheadLog(path)) {
            assertEquals(entries(1, 3), recovered.recover());
            assertEquals(3, recovered.recoveryReport().recoveredEntries());
            recovered.appendUnforced(entry(4));
            recovered.force();
        }
        try (WriteAheadLog recoveredAgain = new WriteAheadLog(path)) {
            assertEquals(4, recoveredAgain.lastIndex());
        }
    }

    @Test
    void incompleteTailIsDiscardedAndFileCanBeAppendedAgain() throws IOException {
        Path path = tempDirectory.resolve("torn.wal");
        try (WriteAheadLog wal = new WriteAheadLog(path)) {
            wal.appendUnforced(entry(1));
            wal.force();
        }
        long goodSize = Files.size(path);
        Files.write(path, new byte[]{1, 2, 3, 4, 5}, StandardOpenOption.APPEND);

        try (WriteAheadLog recovered = new WriteAheadLog(path)) {
            assertEquals(1, recovered.lastIndex());
            assertEquals(5, recovered.recoveryReport().truncatedBytes());
            assertEquals(goodSize, Files.size(path));
            recovered.appendUnforced(entry(2));
            recovered.force();
        }
        try (WriteAheadLog recovered = new WriteAheadLog(path)) {
            assertEquals(2, recovered.lastIndex());
        }
    }

    @Test
    void badChecksumOnFinalFrameIsTailButMiddleCorruptionIsFatal() throws IOException {
        Path finalPath = tempDirectory.resolve("final-corruption.wal");
        writeEntries(finalPath, 2);
        byte[] finalBytes = Files.readAllBytes(finalPath);
        finalBytes[finalBytes.length - 1] ^= 1;
        Files.write(finalPath, finalBytes);
        try (WriteAheadLog recovered = new WriteAheadLog(finalPath)) {
            assertEquals(1, recovered.lastIndex());
            assertTrue(recovered.recoveryReport().truncatedBytes() > 0);
        }

        Path middlePath = tempDirectory.resolve("middle-corruption.wal");
        writeEntries(middlePath, 3);
        byte[] middleBytes = Files.readAllBytes(middlePath);
        int firstPayloadLength = ByteBuffer.wrap(middleBytes, 8, Integer.BYTES).getInt();
        int firstChecksumOffset = 12 + firstPayloadLength;
        middleBytes[firstChecksumOffset] ^= 1;
        Files.write(middlePath, middleBytes);
        assertThrows(WalCorruptionException.class, () -> new WriteAheadLog(middlePath));
    }

    @Test
    void rejectsGapsBeforeWritingAnything() throws IOException {
        Path path = tempDirectory.resolve("gap.wal");
        try (WriteAheadLog wal = new WriteAheadLog(path)) {
            assertThrows(IllegalArgumentException.class, () -> wal.appendUnforced(entry(2)));
            assertEquals(0, wal.lastIndex());
            assertEquals(0, Files.size(path));
        }
    }

    private void writeEntries(Path path, int count) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(path)) {
            wal.appendUnforced(entries(1, count));
            wal.force();
        }
    }

    private static List<RaftLogEntry> entries(long firstIndex, int count) {
        return java.util.stream.LongStream.range(firstIndex, firstIndex + count)
                .mapToObj(WriteAheadLogTest::entry)
                .toList();
    }

    private static RaftLogEntry entry(long index) {
        return new RaftLogEntry(index, 2, RaftCommand.set(
                new byte[]{(byte) index}, new byte[]{(byte) (index + 1)}));
    }
}
