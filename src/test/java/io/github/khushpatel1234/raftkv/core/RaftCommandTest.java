package io.github.khushpatel1234.raftkv.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftCommandTest {
    @Test
    void commandUsesDeepContentEqualityAndDefensiveCopies() {
        byte[] key = {1, 2};
        byte[] value = {3, 4};
        RaftCommand command = RaftCommand.set(key, value);
        RaftCommand equal = RaftCommand.set(new byte[]{1, 2}, new byte[]{3, 4});

        key[0] = 9;
        value[0] = 9;
        byte[] exposedKey = command.keys().getFirst();
        byte[] exposedValue = command.value();
        exposedKey[0] = 8;
        exposedValue[0] = 8;

        assertEquals(equal, command);
        assertEquals(equal.hashCode(), command.hashCode());
        assertArrayEquals(new byte[]{1, 2}, command.key());
        assertArrayEquals(new byte[]{3, 4}, command.value());
        assertNotEquals(command, RaftCommand.set(new byte[]{1, 2}, new byte[]{3, 5}));
    }

    @Test
    void everyCommandRoundTripsThroughDeterministicCodec() {
        List<RaftCommand> commands = List.of(
                RaftCommand.set(new byte[0], new byte[]{0, (byte) 0xff}),
                RaftCommand.delete(new byte[]{1}, new byte[0], new byte[]{2, 3}),
                RaftCommand.noop());

        for (RaftCommand command : commands) {
            byte[] firstEncoding = command.encode();
            assertEquals(command, RaftCommand.decode(firstEncoding));
            assertArrayEquals(firstEncoding, command.encode());
        }
    }

    @Test
    void codecRejectsTruncationTrailingDataAndUnknownVersion() {
        byte[] encoded = RaftCommand.set(new byte[]{1}, new byte[]{2}).encode();
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> RaftCommand.decode(truncated));
        }
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(IllegalArgumentException.class, () -> RaftCommand.decode(trailing));
        byte[] unknownVersion = encoded.clone();
        unknownVersion[4] = 99;
        assertThrows(IllegalArgumentException.class, () -> RaftCommand.decode(unknownVersion));
    }

    @Test
    void logEntryRoundTripsAndValidatesItsCoordinates() {
        RaftLogEntry entry = new RaftLogEntry(7, 3, RaftCommand.delete(new byte[]{9}));
        assertEquals(entry, RaftLogEntry.decode(entry.encode()));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(0, 1, RaftCommand.noop()));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(1, -1, RaftCommand.noop()));
    }
}
