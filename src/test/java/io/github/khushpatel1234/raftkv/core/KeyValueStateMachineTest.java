package io.github.khushpatel1234.raftkv.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueStateMachineTest {
    @Test
    void keysAndValuesAreBinarySafeAndDefensivelyCopied() {
        KeyValueStateMachine stateMachine = new KeyValueStateMachine();
        byte[] key = {0, (byte) 0xff, 7};
        byte[] value = {9, 0, (byte) 0x80};

        stateMachine.set(key, value);
        key[0] = 100;
        value[0] = 100;

        byte[] lookupKey = {0, (byte) 0xff, 7};
        byte[] returned = stateMachine.get(lookupKey);
        assertArrayEquals(new byte[]{9, 0, (byte) 0x80}, returned);
        returned[0] = 42;
        assertArrayEquals(new byte[]{9, 0, (byte) 0x80}, stateMachine.get(lookupKey));
        assertNull(stateMachine.get(key));
    }

    @Test
    void applyReturnsDeterministicAffectedCountsForMultiDelete() {
        KeyValueStateMachine stateMachine = new KeyValueStateMachine();
        byte[] first = "first".getBytes();
        byte[] second = "second".getBytes();

        assertEquals(1L, stateMachine.apply(RaftCommand.set(first, new byte[0])));
        assertEquals(1L, stateMachine.apply(RaftCommand.set(second, new byte[]{2})));
        assertEquals(2, stateMachine.size());
        assertEquals(2L, stateMachine.apply(RaftCommand.delete(first, first, second)));
        assertEquals(0L, stateMachine.apply(RaftCommand.noop()));
        assertEquals(0, stateMachine.size());
        assertFalse(stateMachine.delete(first));
        assertTrue(stateMachine.getOptional(second).isEmpty());
    }

    @Test
    void snapshotsAlsoOwnTheirBytes() {
        KeyValueStateMachine stateMachine = new KeyValueStateMachine();
        stateMachine.set(new byte[]{1}, new byte[]{2});

        List<KeyValueStateMachine.Entry> snapshot = stateMachine.snapshot();
        byte[] snapshotValue = snapshot.getFirst().value();
        snapshotValue[0] = 99;

        assertArrayEquals(new byte[]{2}, stateMachine.get(new byte[]{1}));
    }
}
