package io.github.khushpatel1234.raftkv.raft;

import java.util.concurrent.CompletableFuture;

public interface RaftTransport extends AutoCloseable {
    void start(RaftRpcHandler handler) throws InterruptedException;

    CompletableFuture<RaftRpc.RequestVoteResponse> requestVote(
            int targetNodeId, RaftRpc.RequestVoteRequest request);

    CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(
            int targetNodeId, RaftRpc.AppendEntriesRequest request);

    @Override
    void close();
}
