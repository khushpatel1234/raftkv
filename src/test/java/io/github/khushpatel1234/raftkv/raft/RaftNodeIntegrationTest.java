package io.github.khushpatel1234.raftkv.raft;

import io.github.khushpatel1234.raftkv.core.KeyValueStateMachine;
import io.github.khushpatel1234.raftkv.storage.GroupCommitLog;
import io.github.khushpatel1234.raftkv.storage.RaftMetadataStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftNodeIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void replicatesDurableWritesAndFailsOver() throws Exception {
        var network = new InMemoryRaftNetwork();
        var members = members(3);
        var nodes = new ArrayList<RaftNode>();
        var stateMachines = new LinkedHashMap<Integer, KeyValueStateMachine>();
        try {
            for (int id = 1; id <= 3; id++) {
                var machine = new KeyValueStateMachine();
                stateMachines.put(id, machine);
                var nodeDirectory = temporaryDirectory.resolve("node-" + id);
                var configuration = new RaftConfiguration(
                        id,
                        members,
                        Duration.ofMillis(100 + id * 30L),
                        Duration.ofMillis(220 + id * 30L),
                        Duration.ofMillis(25),
                        Duration.ofMillis(80),
                        Duration.ofSeconds(2));
                var node = new RaftNode(
                        configuration,
                        network.transport(id),
                        new GroupCommitLog(nodeDirectory.resolve("raft.wal"), 64, Duration.ofMillis(1)),
                        new RaftMetadataStore(nodeDirectory.resolve("raft.meta")),
                        machine);
                nodes.add(node);
                node.start();
            }

            var firstLeader = awaitLeader(nodes, List.of());
            firstLeader.set(bytes("alpha"), bytes("one")).get(3, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    stateMachines.values().forEach(machine ->
                            assertArrayEquals(bytes("one"), machine.get(bytes("alpha")))));
            assertArrayEquals(bytes("one"),
                    firstLeader.get(bytes("alpha")).get(3, TimeUnit.SECONDS));

            int oldLeaderId = firstLeader.status().nodeId();
            network.isolate(oldLeaderId);
            var secondLeader = awaitLeader(nodes, List.of(oldLeaderId));
            secondLeader.set(bytes("beta"), bytes("two")).get(3, TimeUnit.SECONDS);
            assertArrayEquals(bytes("two"),
                    secondLeader.get(bytes("beta")).get(3, TimeUnit.SECONDS));

            network.heal();
            await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
                assertEquals(1, nodes.stream()
                        .filter(node -> node.status().role() == RaftRole.LEADER)
                        .count());
                stateMachines.values().forEach(machine ->
                        assertArrayEquals(bytes("two"), machine.get(bytes("beta"))));
            });
        } finally {
            nodes.forEach(RaftNode::close);
        }
    }

    private static RaftNode awaitLeader(List<RaftNode> nodes, List<Integer> excludedIds) {
        await().atMost(Duration.ofSeconds(4)).until(() -> nodes.stream()
                .filter(node -> !excludedIds.contains(node.status().nodeId()))
                .filter(node -> node.status().role() == RaftRole.LEADER)
                .count() == 1);
        return nodes.stream()
                .filter(node -> !excludedIds.contains(node.status().nodeId()))
                .filter(node -> node.status().role() == RaftRole.LEADER)
                .findFirst()
                .orElseThrow();
    }

    private static Map<Integer, InetSocketAddress> members(int count) {
        var members = new LinkedHashMap<Integer, InetSocketAddress>();
        for (int id = 1; id <= count; id++) {
            members.put(id, InetSocketAddress.createUnresolved("node-" + id, 7_000 + id));
        }
        return Map.copyOf(members);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
