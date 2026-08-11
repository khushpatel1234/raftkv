package io.github.khushpatel1234.raftkv.raft;

import io.github.khushpatel1234.raftkv.core.RaftCommand;
import io.github.khushpatel1234.raftkv.core.RaftLogEntry;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NettyRaftTransportTest {
    @Test
    void exchangesVoteAndAppendRpcsOverPersistentConnections() throws Exception {
        int firstPort = freePort();
        int secondPort = freePort();
        var members = Map.of(
                1, new InetSocketAddress("127.0.0.1", firstPort),
                2, new InetSocketAddress("127.0.0.1", secondPort));
        try (var first = new NettyRaftTransport(
                1, members.get(1), members, Duration.ofSeconds(2));
             var second = new NettyRaftTransport(
                     2, members.get(2), members, Duration.ofSeconds(2))) {
            first.start(new StubHandler());
            second.start(new StubHandler());

            var vote = first.requestVote(2,
                    new RaftRpc.RequestVoteRequest(4, 1, 9, 3))
                    .get(3, TimeUnit.SECONDS);
            assertEquals(4, vote.term());
            assertTrue(vote.voteGranted());

            var entry = new RaftLogEntry(10, 4, RaftCommand.set(bytes("key"), bytes("value")));
            var append = first.appendEntries(2, new RaftRpc.AppendEntriesRequest(
                            4, 1, 9, 3, List.of(entry), 9))
                    .get(3, TimeUnit.SECONDS);
            assertTrue(append.success());
            assertEquals(10, append.matchIndex());
        }
    }

    @Test
    void retriesAConnectionThatFailedBeforeThePeerStarted() throws Exception {
        int firstPort = freePort();
        int secondPort = freePort();
        var members = Map.of(
                1, new InetSocketAddress("127.0.0.1", firstPort),
                2, new InetSocketAddress("127.0.0.1", secondPort));
        try (var first = new NettyRaftTransport(
                1, members.get(1), members, Duration.ofMillis(300));
             var second = new NettyRaftTransport(
                     2, members.get(2), members, Duration.ofMillis(300))) {
            first.start(new StubHandler());
            var request = new RaftRpc.RequestVoteRequest(1, 1, 0, 0);

            assertThrows(ExecutionException.class,
                    () -> first.requestVote(2, request).get(2, TimeUnit.SECONDS));

            second.start(new StubHandler());
            var response = first.requestVote(2, request).get(2, TimeUnit.SECONDS);
            assertTrue(response.voteGranted());
        }
    }

    @Test
    void closesAndReconnectsAChannelAfterAnRpcTimeout() throws Exception {
        int firstPort = freePort();
        int secondPort = freePort();
        var members = Map.of(
                1, new InetSocketAddress("127.0.0.1", firstPort),
                2, new InetSocketAddress("127.0.0.1", secondPort));
        try (var first = new NettyRaftTransport(
                1, members.get(1), members, Duration.ofMillis(100));
             var second = new NettyRaftTransport(
                     2, members.get(2), members, Duration.ofMillis(100))) {
            first.start(new StubHandler());
            second.start(new TimeoutOnceHandler());
            var request = new RaftRpc.RequestVoteRequest(1, 1, 0, 0);

            assertThrows(ExecutionException.class,
                    () -> first.requestVote(2, request).get(1, TimeUnit.SECONDS));

            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertTrue(first.requestVote(2, request)
                            .get(1, TimeUnit.SECONDS).voteGranted()));
        }
    }

    private static int freePort() throws Exception {
        try (var socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static final class StubHandler implements RaftRpcHandler {
        @Override
        public CompletableFuture<RaftRpc.RequestVoteResponse> requestVote(
                RaftRpc.RequestVoteRequest request) {
            return CompletableFuture.completedFuture(
                    new RaftRpc.RequestVoteResponse(request.term(), true));
        }

        @Override
        public CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(
                RaftRpc.AppendEntriesRequest request) {
            long match = request.prevLogIndex() + request.entries().size();
            return CompletableFuture.completedFuture(
                    new RaftRpc.AppendEntriesResponse(request.term(), true, match, match + 1));
        }
    }

    private static final class TimeoutOnceHandler implements RaftRpcHandler {
        private final AtomicInteger voteRequests = new AtomicInteger();

        @Override
        public CompletableFuture<RaftRpc.RequestVoteResponse> requestVote(
                RaftRpc.RequestVoteRequest request) {
            if (voteRequests.getAndIncrement() == 0) {
                return new CompletableFuture<>();
            }
            return CompletableFuture.completedFuture(
                    new RaftRpc.RequestVoteResponse(request.term(), true));
        }

        @Override
        public CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(
                RaftRpc.AppendEntriesRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
