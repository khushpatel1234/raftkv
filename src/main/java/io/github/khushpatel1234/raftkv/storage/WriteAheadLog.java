package io.github.khushpatel1234.raftkv.storage;

import io.github.khushpatel1234.raftkv.core.RaftLogEntry;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Append-only, checksummed storage for a contiguous Raft log.
 *
 * <p>Opening the log verifies every complete frame. An incomplete frame or a
 * checksum-bad final frame is considered a torn tail and is truncated. A bad
 * checksum followed by more data is reported as corruption. Prefix compaction
 * is intentionally unsupported; snapshots can add it later without changing
 * the on-disk frame format.</p>
 */
public final class WriteAheadLog implements AutoCloseable {
    private static final int FRAME_MAGIC = 0x524B574C; // RKWL
    private static final short FRAME_VERSION = 1;
    private static final short FRAME_FLAGS = 0;
    private static final int HEADER_SIZE = Integer.BYTES + Short.BYTES + Short.BYTES + Integer.BYTES;
    private static final int CHECKSUM_SIZE = Integer.BYTES;
    private static final int MAX_PAYLOAD_SIZE = 64 * 1024 * 1024;

    private final Path path;
    private final FileChannel channel;
    private final List<RaftLogEntry> entries = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>();
    private final RecoveryReport recoveryReport;
    private boolean closed;

    public WriteAheadLog(Path path) throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath();
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        channel = FileChannel.open(this.path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        RecoveryReport report;
        try {
            report = recoverInternal();
        } catch (IOException | RuntimeException exception) {
            channel.close();
            throw exception;
        }
        recoveryReport = report;
    }

    public static WriteAheadLog open(Path path) throws IOException {
        return new WriteAheadLog(path);
    }

    public Path path() {
        return path;
    }

    public RecoveryReport recoveryReport() {
        return recoveryReport;
    }

    public synchronized List<RaftLogEntry> recover() {
        return List.copyOf(entries);
    }

    public synchronized List<RaftLogEntry> entries() {
        return List.copyOf(entries);
    }

    public synchronized long lastIndex() {
        return entries.isEmpty() ? 0L : entries.getLast().index();
    }

    public synchronized Optional<RaftLogEntry> entry(long index) {
        if (index < 1 || index > entries.size()) {
            return Optional.empty();
        }
        return Optional.of(entries.get(Math.toIntExact(index - 1)));
    }

    public synchronized List<RaftLogEntry> entriesFrom(long fromIndexInclusive, int maxEntries) {
        if (fromIndexInclusive < 1) {
            throw new IllegalArgumentException("fromIndexInclusive must be at least 1");
        }
        if (maxEntries < 0) {
            throw new IllegalArgumentException("maxEntries cannot be negative");
        }
        if (maxEntries == 0 || fromIndexInclusive > entries.size()) {
            return List.of();
        }
        int from = Math.toIntExact(fromIndexInclusive - 1);
        int to = (int) Math.min((long) entries.size(), (long) from + maxEntries);
        return List.copyOf(entries.subList(from, to));
    }

    /**
     * Appends bytes without forcing them to stable storage. Callers must invoke
     * {@link #force()} before reporting durability; {@link GroupCommitLog} does
     * this once for a whole batch.
     */
    public synchronized void appendUnforced(RaftLogEntry entry) throws IOException {
        appendUnforced(List.of(entry));
    }

    /** Appends a contiguous list without an fsync. */
    public synchronized void appendUnforced(List<RaftLogEntry> newEntries) throws IOException {
        ensureOpen();
        Objects.requireNonNull(newEntries, "newEntries");
        if (newEntries.isEmpty()) {
            return;
        }

        long expectedIndex = lastIndex() + 1;
        List<byte[]> frames = new ArrayList<>(newEntries.size());
        for (RaftLogEntry entry : newEntries) {
            Objects.requireNonNull(entry, "entry");
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException(
                        "Expected log index " + expectedIndex + " but received " + entry.index());
            }
            frames.add(frame(entry));
            expectedIndex++;
        }

        List<Long> newOffsets = new ArrayList<>(frames.size());
        long offset = channel.size();
        channel.position(offset);
        for (byte[] frame : frames) {
            newOffsets.add(offset);
            writeFully(channel, ByteBuffer.wrap(frame));
            offset += frame.length;
        }
        entries.addAll(newEntries);
        offsets.addAll(newOffsets);
    }

    /** Forces all preceding appends to the storage device without metadata fsync. */
    public synchronized void force() throws IOException {
        ensureOpen();
        channel.force(false);
    }

    /**
     * Removes {@code fromIndexInclusive} and every later entry, and forces the
     * truncation before returning.
     */
    public synchronized void truncateSuffix(long fromIndexInclusive) throws IOException {
        ensureOpen();
        if (fromIndexInclusive < 1) {
            throw new IllegalArgumentException("fromIndexInclusive must be at least 1");
        }
        if (fromIndexInclusive > entries.size()) {
            return;
        }
        int from = Math.toIntExact(fromIndexInclusive - 1);
        long offset = offsets.get(from);
        channel.truncate(offset);
        channel.position(offset);
        channel.force(false);
        entries.subList(from, entries.size()).clear();
        offsets.subList(from, offsets.size()).clear();
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            try {
                channel.force(false);
            } finally {
                closed = true;
                channel.close();
            }
        }
    }

    private RecoveryReport recoverInternal() throws IOException {
        long fileSize = channel.size();
        long offset = 0L;
        long expectedIndex = 1L;
        while (offset < fileSize) {
            long remaining = fileSize - offset;
            if (remaining < HEADER_SIZE) {
                return truncateRecoveredTail(fileSize, offset);
            }

            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            readFully(channel, header, offset);
            byte[] headerBytes = header.array();
            header.flip();
            int magic = header.getInt();
            int version = Short.toUnsignedInt(header.getShort());
            int flags = Short.toUnsignedInt(header.getShort());
            int payloadLength = header.getInt();
            if (magic != FRAME_MAGIC) {
                throw new WalCorruptionException("Invalid WAL frame magic at offset " + offset);
            }
            if (version != FRAME_VERSION || flags != FRAME_FLAGS) {
                throw new WalCorruptionException("Unsupported WAL frame header at offset " + offset);
            }
            if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_SIZE) {
                throw new WalCorruptionException("Invalid WAL payload length at offset " + offset);
            }
            long frameSize = HEADER_SIZE + (long) payloadLength + CHECKSUM_SIZE;
            if (remaining < frameSize) {
                return truncateRecoveredTail(fileSize, offset);
            }

            ByteBuffer payload = ByteBuffer.allocate(payloadLength);
            readFully(channel, payload, offset + HEADER_SIZE);
            byte[] payloadBytes = payload.array();
            ByteBuffer checksumBuffer = ByteBuffer.allocate(CHECKSUM_SIZE);
            readFully(channel, checksumBuffer, offset + HEADER_SIZE + payloadLength);
            checksumBuffer.flip();
            int storedChecksum = checksumBuffer.getInt();
            int calculatedChecksum = checksum(headerBytes, payloadBytes);
            if (storedChecksum != calculatedChecksum) {
                if (remaining == frameSize) {
                    return truncateRecoveredTail(fileSize, offset);
                }
                throw new WalCorruptionException("WAL checksum mismatch at offset " + offset);
            }

            RaftLogEntry entry;
            try {
                entry = RaftLogEntry.decode(payloadBytes);
            } catch (IllegalArgumentException exception) {
                throw new WalCorruptionException("Invalid WAL entry at offset " + offset, exception);
            }
            if (entry.index() != expectedIndex) {
                throw new WalCorruptionException(
                        "Expected recovered log index " + expectedIndex + " but found " + entry.index());
            }
            entries.add(entry);
            offsets.add(offset);
            expectedIndex++;
            offset += frameSize;
        }
        channel.position(fileSize);
        return new RecoveryReport(entries.size(), 0L);
    }

    private RecoveryReport truncateRecoveredTail(long originalSize, long goodSize) throws IOException {
        long truncatedBytes = originalSize - goodSize;
        channel.truncate(goodSize);
        channel.position(goodSize);
        channel.force(false);
        return new RecoveryReport(entries.size(), truncatedBytes);
    }

    private static byte[] frame(RaftLogEntry entry) {
        byte[] payload = entry.encode();
        if (payload.length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("WAL entry exceeds " + MAX_PAYLOAD_SIZE + " bytes");
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(FRAME_MAGIC);
        header.putShort(FRAME_VERSION);
        header.putShort(FRAME_FLAGS);
        header.putInt(payload.length);
        byte[] headerBytes = header.array();

        ByteBuffer output = ByteBuffer.allocate(HEADER_SIZE + payload.length + CHECKSUM_SIZE);
        output.put(headerBytes);
        output.put(payload);
        output.putInt(checksum(headerBytes, payload));
        return output.array();
    }

    private static int checksum(byte[] header, byte[] payload) {
        CRC32C checksum = new CRC32C();
        checksum.update(header, 0, header.length);
        checksum.update(payload, 0, payload.length);
        return (int) checksum.getValue();
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new EOFException("Unexpected end of WAL");
            }
            if (read == 0) {
                Thread.onSpinWait();
            } else {
                position += read;
            }
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);
            if (written == 0) {
                Thread.onSpinWait();
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("WAL is closed");
        }
    }

    public record RecoveryReport(int recoveredEntries, long truncatedBytes) {
        public RecoveryReport {
            if (recoveredEntries < 0 || truncatedBytes < 0) {
                throw new IllegalArgumentException("Recovery counters cannot be negative");
            }
        }
    }
}
