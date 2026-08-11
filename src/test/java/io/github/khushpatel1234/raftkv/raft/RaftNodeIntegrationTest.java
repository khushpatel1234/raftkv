package io.github.khushpatel1234.raftkv.raft;

import io.github.khushpatel1234.raftkv.core.KeyValueStateMachine;
import io.github.khushpatel1234.raftkv.core.RaftCommand;
import io.github.khushpatel1234.raftkv.core.RaftLogEntry;
import io.github.khushpatel1234.raftkv.storage.GroupCommitLog;
import io.github.khushpatel1234.raftkv.storage.RaftMetadata;
import io.github.khushpatel1234.raftkv.storage.RaftMetadataStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            await().atMost(Duration.ofSeconds(2)).until(() ->
                    firstLeader.status().role() != RaftRole.LEADER);
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

    @Test
    void publicOperationsFailPromptlyAfterClose() throws Exception {
        var nodeDirectory = temporaryDirectory.resolve("closed-node");
        var configuration = new RaftConfiguration(
                1,
                members(1),
                Duration.ofMillis(100),
                Duration.ofMillis(200),
                Duration.ofMillis(25),
                Duration.ofMillis(80),
                Duration.ofSeconds(2));
        var node = new RaftNode(
                configuration,
                new InMemoryRaftNetwork().transport(1),
                new GroupCommitLog(nodeDirectory.resolve("raft.wal")),
                new RaftMetadataStore(nodeDirectory.resolve("raft.meta")),
                new KeyValueStateMachine());
        node.start();
        node.close();

        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
            assertTrue(node.get(bytes("key")).isCompletedExceptionally());
            assertTrue(node.set(bytes("key"), bytes("value")).isCompletedExceptionally());
            assertTrue(node.requestVote(new RaftRpc.RequestVoteRequest(1, 2, 0, 0))
                    .isCompletedExceptionally());
            assertTrue(node.appendEntries(new RaftRpc.AppendEntriesRequest(
                    1, 2, 0, 0, List.of(), 0)).isCompletedExceptionally());
        });
    }

    @Test
    void followerCommitNeverAdvancesPastThePrefixProvenByAppendEntries() throws Exception {
        var nodeDirectory = temporaryDirectory.resolve("divergent-follower");
        var walPath = nodeDirectory.resolve("raft.wal");
        var divergent = new ArrayList<RaftLogEntry>();
        for (long index = 1; index <= 100; index++) {
            divergent.add(new RaftLogEntry(index, 1, RaftCommand.set(
                    bytes("key-" + index), bytes("old-" + index))));
        }
        try (var seedLog = new GroupCommitLog(walPath)) {
            seedLog.append(divergent).get(3, TimeUnit.SECONDS);
        }
        var metadataPath = nodeDirectory.resolve("raft.meta");
        new RaftMetadataStore(metadataPath).save(new RaftMetadata(1, null, 0));

        var machine = new KeyValueStateMachine();
        var configuration = new RaftConfiguration(
                1,
                members(2),
                Duration.ofMillis(100),
                Duration.ofMillis(200),
                Duration.ofMillis(25),
                Duration.ofMillis(80),
                Duration.ofSeconds(2));
        var node = new RaftNode(
                configuration,
                new InMemoryRaftNetwork().transport(1),
                new GroupCommitLog(walPath),
                new RaftMetadataStore(metadataPath),
                machine);
        try {
            var readProbe = node.appendEntries(new RaftRpc.AppendEntriesRequest(
                    2, 2, 0, 0, List.of(), 100)).get(3, TimeUnit.SECONDS);
            assertTrue(readProbe.success());
            assertEquals(0, node.status().commitIndex());
            assertEquals(0, machine.size());

            var prefix = node.appendEntries(new RaftRpc.AppendEntriesRequest(
                    2, 2, 0, 0, divergent.subList(0, 64), 100)).get(3, TimeUnit.SECONDS);
            assertTrue(prefix.success());
            assertEquals(64, prefix.matchIndex());
            assertEquals(64, node.status().commitIndex());
            assertEquals(64, machine.size());
        } finally {
            node.close();
        }
    }

    @Test
    void recoveryRejectsMetadataWhoseTermRolledBehindTheLog() throws Exception {
        var nodeDirectory = temporaryDirectory.resolve("rolled-back-metadata");
        var walPath = nodeDirectory.resolve("raft.wal");
        try (var seedLog = new GroupCommitLog(walPath)) {
            seedLog.append(new RaftLogEntry(
                    1, 3, RaftCommand.set(bytes("key"), bytes("value"))))
                    .get(3, TimeUnit.SECONDS);
        }

        var configuration = new RaftConfiguration(
                1,
                members(1),
                Duration.ofMillis(100),
                Duration.ofMillis(200),
                Duration.ofMillis(25),
                Duration.ofMillis(80),
                Duration.ofSeconds(2));
        try (var recoveredLog = new GroupCommitLog(walPath)) {
            assertThrows(IOException.class, () -> new RaftNode(
                    configuration,
                    new InMemoryRaftNetwork().transport(1),
                    recoveredLog,
                    new RaftMetadataStore(nodeDirectory.resolve("raft.meta")),
                    new KeyValueStateMachine()));
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
