package io.github.khushpatel1234.raftkv.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftMetadataStoreTest {
    @TempDir
    Path tempDirectory;

    @Test
    void absentStoreStartsAtSafeInitialStateAndTupleSurvivesRestart() throws IOException {
        Path path = tempDirectory.resolve("metadata.bin");
        RaftMetadataStore store = new RaftMetadataStore(path);
        assertEquals(RaftMetadata.initial(), store.load());

        store.updateVote(4, "node-β");
        store.updateCommitIndex(19);
        assertEquals(new RaftMetadata(4, "node-β", 19), store.load());
        assertEquals(store.load(), new RaftMetadataStore(path).load());
    }

    @Test
    void newTermClearsVoteAndSafetyFieldsCannotMoveBackwards() throws IOException {
        RaftMetadataStore store = new RaftMetadataStore(tempDirectory.resolve("safe.bin"));
        store.updateVote(2, "first");
        assertThrows(IllegalArgumentException.class, () -> store.updateVote(2, "second"));
        assertThrows(IllegalArgumentException.class, () -> store.updateTerm(1));
        store.updateCommitIndex(8);
        assertThrows(IllegalArgumentException.class, () -> store.updateCommitIndex(7));

        RaftMetadata next = store.updateTerm(3);
        assertEquals(3, next.term());
        assertNull(next.votedFor());
        assertEquals(8, next.commitIndex());
    }

    @Test
    void corruptMetadataNeverSilentlyResetsSafetyState() throws IOException {
        Path path = tempDirectory.resolve("corrupt.bin");
        RaftMetadataStore store = new RaftMetadataStore(path);
        store.save(new RaftMetadata(9, "node-a", 12));

        byte[] bytes = Files.readAllBytes(path);
        bytes[bytes.length - 1] ^= 1;
        Files.write(path, bytes);

        assertThrows(IOException.class, () -> new RaftMetadataStore(path));
    }
}
