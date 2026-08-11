package io.github.khushpatel1234.raftkv.raft;

import java.util.concurrent.CompletableFuture;

public interface RaftRpcHandler {
    CompletableFuture<RaftRpc.RequestVoteResponse> requestVote(RaftRpc.RequestVoteRequest request);

    CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(RaftRpc.AppendEntriesRequest request);
}
