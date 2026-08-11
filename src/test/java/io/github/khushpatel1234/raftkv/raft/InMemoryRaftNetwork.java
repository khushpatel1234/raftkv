package io.github.khushpatel1234.raftkv.raft;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

final class InMemoryRaftNetwork {
    private final Map<Integer, RaftRpcHandler> handlers = new ConcurrentHashMap<>();
    private final Set<Link> blocked = ConcurrentHashMap.newKeySet();
    private final Executor executor;

    InMemoryRaftNetwork() {
        this(ForkJoinPool.commonPool());
    }

    InMemoryRaftNetwork(Executor executor) {
        this.executor = executor;
    }

    RaftTransport transport(int nodeId) {
        return new Transport(nodeId);
    }

    void isolate(int nodeId) {
        for (int other : handlers.keySet()) {
            if (other != nodeId) {
                block(nodeId, other);
                block(other, nodeId);
            }
        }
    }

    void heal() {
        blocked.clear();
    }

    void block(int from, int to) {
        blocked.add(new Link(from, to));
    }

    private <T> CompletableFuture<T> invoke(int from, int to, RpcCall<T> call) {
        if (blocked.contains(new Link(from, to))) {
            return CompletableFuture.failedFuture(new IllegalStateException("link is blocked"));
        }
        var handler = handlers.get(to);
        if (handler == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("node " + to + " is offline"));
        }
        return CompletableFuture.supplyAsync(() -> handler, executor).thenCompose(call::invoke);
    }

    private record Link(int from, int to) {
    }

    @FunctionalInterface
    private interface RpcCall<T> {
        CompletableFuture<T> invoke(RaftRpcHandler handler);
    }

    private final class Transport implements RaftTransport {
        private final int nodeId;

        private Transport(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public void start(RaftRpcHandler handler) {
            if (handlers.putIfAbsent(nodeId, handler) != null) {
                throw new IllegalStateException("node " + nodeId + " is already registered");
            }
        }

        @Override
        public CompletableFuture<RaftRpc.RequestVoteResponse> requestVote(
                int targetNodeId, RaftRpc.RequestVoteRequest request) {
            return invoke(nodeId, targetNodeId, handler -> handler.requestVote(request));
        }

        @Override
        public CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(
                int targetNodeId, RaftRpc.AppendEntriesRequest request) {
            return invoke(nodeId, targetNodeId, handler -> handler.appendEntries(request));
        }

        @Override
        public void close() {
            handlers.remove(nodeId);
        }
    }
}
