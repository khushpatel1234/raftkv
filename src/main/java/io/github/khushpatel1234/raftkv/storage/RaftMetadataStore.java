package io.github.khushpatel1234.raftkv.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Persists term, vote, and commit index as one checksummed, atomically replaced
 * record. State is published in memory only after the replacement succeeds.
 */
public final class RaftMetadataStore {
    private static final int MAGIC = 0x524B4D44; // RKMD
    private static final short VERSION = 1;
    private static final short FLAGS = 0;
    private static final int FIXED_PREFIX_SIZE =
            Integer.BYTES + Short.BYTES + Short.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES;
    private static final int CHECKSUM_SIZE = Integer.BYTES;
    private static final int MAX_VOTE_BYTES = 64 * 1024;

    private final Path path;
    private RaftMetadata current;

    public RaftMetadataStore(Path path) throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath();
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        current = read();
    }

    public synchronized RaftMetadata load() {
        return current;
    }

    /** Saves a legal monotonic transition as one durable record. */
    public synchronized void save(RaftMetadata next) throws IOException {
        Objects.requireNonNull(next, "next");
        validateTransition(current, next);
        persist(next);
        current = next;
    }

    public synchronized RaftMetadata updateTerm(long newTerm) throws IOException {
        if (newTerm < current.term()) {
            throw new IllegalArgumentException("term cannot move backwards");
        }
        if (newTerm == current.term()) {
            return current;
        }
        RaftMetadata next = new RaftMetadata(newTerm, null, current.commitIndex());
        persist(next);
        current = next;
        return next;
    }

    public synchronized RaftMetadata updateVote(String candidateId) throws IOException {
        return updateVote(current.term(), candidateId);
    }

    /** Atomically advances to {@code term}, if needed, and records its vote. */
    public synchronized RaftMetadata updateVote(long term, String candidateId) throws IOException {
        Objects.requireNonNull(candidateId, "candidateId");
        RaftMetadata next = new RaftMetadata(term, candidateId, current.commitIndex());
        validateTransition(current, next);
        if (next.equals(current)) {
            return current;
        }
        persist(next);
        current = next;
        return next;
    }

    public synchronized RaftMetadata updateCommitIndex(long commitIndex) throws IOException {
        RaftMetadata next = new RaftMetadata(current.term(), current.votedFor(), commitIndex);
        validateTransition(current, next);
        if (next.equals(current)) {
            return current;
        }
        persist(next);
        current = next;
        return next;
    }

    private RaftMetadata read() throws IOException {
        if (!Files.exists(path)) {
            return RaftMetadata.initial();
        }
        byte[] encoded = Files.readAllBytes(path);
        if (encoded.length < FIXED_PREFIX_SIZE + CHECKSUM_SIZE) {
            throw new IOException("Truncated Raft metadata file: " + path);
        }
        ByteBuffer input = ByteBuffer.wrap(encoded);
        int magic = input.getInt();
        int version = Short.toUnsignedInt(input.getShort());
        int flags = Short.toUnsignedInt(input.getShort());
        long term = input.getLong();
        long commitIndex = input.getLong();
        int voteLength = input.getInt();
        if (magic != MAGIC || version != VERSION || flags != FLAGS) {
            throw new IOException("Invalid or unsupported Raft metadata header: " + path);
        }
        if (voteLength < -1 || voteLength > MAX_VOTE_BYTES) {
            throw new IOException("Invalid vote length in Raft metadata: " + voteLength);
        }
        int expectedRemaining = (voteLength < 0 ? 0 : voteLength) + CHECKSUM_SIZE;
        if (input.remaining() != expectedRemaining) {
            throw new IOException("Invalid Raft metadata record length");
        }
        String votedFor = null;
        if (voteLength >= 0) {
            byte[] vote = new byte[voteLength];
            input.get(vote);
            votedFor = new String(vote, StandardCharsets.UTF_8);
        }
        int storedChecksum = input.getInt();
        CRC32C checksum = new CRC32C();
        checksum.update(encoded, 0, encoded.length - CHECKSUM_SIZE);
        if (storedChecksum != (int) checksum.getValue()) {
            throw new IOException("Raft metadata checksum mismatch: " + path);
        }
        try {
            return new RaftMetadata(term, votedFor, commitIndex);
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid Raft metadata values", exception);
        }
    }

    private void persist(RaftMetadata metadata) throws IOException {
        byte[] vote = metadata.votedFor() == null
                ? null
                : metadata.votedFor().getBytes(StandardCharsets.UTF_8);
        if (vote != null && vote.length > MAX_VOTE_BYTES) {
            throw new IllegalArgumentException("votedFor exceeds " + MAX_VOTE_BYTES + " UTF-8 bytes");
        }
        int voteLength = vote == null ? -1 : vote.length;
        int encodedSize = FIXED_PREFIX_SIZE + (vote == null ? 0 : vote.length) + CHECKSUM_SIZE;
        ByteBuffer output = ByteBuffer.allocate(encodedSize);
        output.putInt(MAGIC);
        output.putShort(VERSION);
        output.putShort(FLAGS);
        output.putLong(metadata.term());
        output.putLong(metadata.commitIndex());
        output.putInt(voteLength);
        if (vote != null) {
            output.put(vote);
        }
        CRC32C checksum = new CRC32C();
        checksum.update(output.array(), 0, encodedSize - CHECKSUM_SIZE);
        output.putInt((int) checksum.getValue());

        Path parent = path.getParent();
        if (parent == null) {
            throw new IOException("Metadata path has no parent: " + path);
        }
        Path temporary = parent.resolve(path.getFileName() + ".tmp-" + UUID.randomUUID());
        try {
            try (FileChannel channel = FileChannel.open(
                    temporary,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                output.flip();
                writeFully(channel, output);
                channel.force(true);
            }
            try {
                Files.move(temporary, path,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic metadata replacement is not supported", exception);
            }
            forceDirectory(parent);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void validateTransition(RaftMetadata previous, RaftMetadata next) {
        if (next.term() < previous.term()) {
            throw new IllegalArgumentException("term cannot move backwards");
        }
        if (next.commitIndex() < previous.commitIndex()) {
            throw new IllegalArgumentException("commitIndex cannot move backwards");
        }
        if (next.term() == previous.term() && previous.votedFor() != null
                && !previous.votedFor().equals(next.votedFor())) {
            throw new IllegalArgumentException("vote cannot change or be cleared within a term");
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

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (UnsupportedOperationException unsupported) {
            throw new IOException(
                    "Directory fsync is required for durable Raft metadata: " + directory,
                    unsupported);
        }
    }
}
