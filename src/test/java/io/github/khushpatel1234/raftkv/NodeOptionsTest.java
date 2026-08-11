package io.github.khushpatel1234.raftkv;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeOptionsTest {
    @Test
    void parsesFullStaticMembership() {
        var options = NodeOptions.parse(new String[] {
                "--node-id", "2",
                "--client-port=6381",
                "--raft-port", "7001",
                "--peers", "1=one:7000,2=two:7001,3=three:7002"
        });

        assertEquals(2, options.nodeId());
        assertEquals(6381, options.clientPort());
        assertEquals(3, options.members().size());
        assertEquals("three", options.members().get(3).getHostString());
    }

    @Test
    void requiresTheLocalNodeInMembership() {
        assertThrows(IllegalArgumentException.class, () -> NodeOptions.parse(new String[] {
                "--node-id", "2", "--peers", "1=localhost:7000"
        }));
    }

    @Test
    void rejectsUnknownOptionsAndBadPorts() {
        assertThrows(IllegalArgumentException.class,
                () -> NodeOptions.parse(new String[] {"--wat", "1"}));
        assertThrows(IllegalArgumentException.class,
                () -> NodeOptions.parse(new String[] {"--client-port", "70000"}));
    }
}
