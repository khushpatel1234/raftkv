package io.github.khushpatel1234.raftkv;

import io.github.khushpatel1234.raftkv.raft.NotLeaderException;
import io.github.khushpatel1234.raftkv.raft.QuorumUnavailableException;
import io.github.khushpatel1234.raftkv.raft.RaftNode;
import io.github.khushpatel1234.raftkv.resp.RespResponse;
import io.github.khushpatel1234.raftkv.server.Command;
import io.github.khushpatel1234.raftkv.server.CommandDispatcher;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

final class RaftCommandDispatcher implements CommandDispatcher {
    private final RaftNode node;

    RaftCommandDispatcher(RaftNode node) {
        this.node = Objects.requireNonNull(node, "node");
    }

    @Override
    public CompletionStage<RespResponse> dispatch(Command command) {
        CompletionStage<RespResponse> response = switch (command) {
            case Command.Get get -> node.get(get.key())
                    .thenApply(value -> value == null ? RespResponse.nullBulk() : RespResponse.bulk(value));
            case Command.Set set -> node.set(set.key(), set.value())
                    .thenApply(ignored -> RespResponse.simple("OK"));
            case Command.Del del -> node.delete(del.keys()).thenApply(RespResponse::integer);
            case Command.Info ignored -> CompletableFuture.completedFuture(info());
            case Command.GroupStats ignored -> CompletableFuture.completedFuture(groupStats());
            case Command.Ping ignored -> CompletableFuture.completedFuture(RespResponse.simple("PONG"));
            case Command.Echo echo -> CompletableFuture.completedFuture(RespResponse.bulk(echo.message()));
        };
        return response.handle((value, error) -> error == null ? value : protocolError(error));
    }

    private RespResponse info() {
        var status = node.status();
        var body = """
                # Raft
                node_id:%d
                role:%s
                term:%d
                leader_id:%s
                commit_index:%d
                last_applied:%d
                last_log_index:%d
                cluster_size:%d
                """.formatted(
                status.nodeId(),
                status.role().name().toLowerCase(java.util.Locale.ROOT),
                status.term(),
                status.leaderId() == null ? "unknown" : status.leaderId(),
                status.commitIndex(),
                status.lastApplied(),
                status.lastLogIndex(),
                status.clusterSize());
        return RespResponse.bulk(body);
    }

    private RespResponse groupStats() {
        var metrics = node.groupCommitMetrics();
        var body = """
                # Group commit
                batch_count:%d
                entry_count:%d
                max_batch_size:%d
                average_batch_size:%.2f
                """.formatted(
                metrics.batchCount(),
                metrics.entryCount(),
                metrics.maxBatchSize(),
                metrics.averageBatchSize());
        return RespResponse.bulk(body);
    }

    private static RespResponse protocolError(Throwable failure) {
        var error = unwrap(failure);
        if (error instanceof NotLeaderException notLeader) {
            var leader = notLeader.leaderId() == null ? "unknown" : notLeader.leaderId();
            return RespResponse.error("ERR NOTLEADER leader_id=" + leader);
        }
        if (error instanceof QuorumUnavailableException || error instanceof TimeoutException) {
            return RespResponse.error("TRYAGAIN quorum unavailable");
        }
        return RespResponse.error("ERR " + safeMessage(error));
    }

    private static Throwable unwrap(Throwable error) {
        var current = error;
        while ((current instanceof CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String safeMessage(Throwable error) {
        var message = error.getMessage();
        if (message == null || message.isBlank()) {
            message = error.getClass().getSimpleName();
        }
        return message.replace('\r', ' ').replace('\n', ' ');
    }
}
