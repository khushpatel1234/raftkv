package io.github.khushpatel1234.raftkv.raft;

import io.github.khushpatel1234.raftkv.core.KeyValueStateMachine;
import io.github.khushpatel1234.raftkv.core.RaftCommand;
import io.github.khushpatel1234.raftkv.core.RaftLogEntry;
import io.github.khushpatel1234.raftkv.storage.GroupCommitLog;
import io.github.khushpatel1234.raftkv.storage.GroupCommitMetrics;
import io.github.khushpatel1234.raftkv.storage.RaftMetadata;
import io.github.khushpatel1234.raftkv.storage.RaftMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single-threaded implementation of the core Raft state machine.
 *
 * <p>Disk I/O is handed to {@link GroupCommitLog}; transport callbacks are always marshalled
 * back onto this node's executor. Client futures are completed only after an entry is durable on
 * a majority and has been applied locally.</p>
 */
public final class RaftNode implements RaftRpcHandler, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);
    private static final int MAX_APPEND_BATCH = 64;

    private final RaftConfiguration configuration;
    private final RaftTransport transport;
    private final GroupCommitLog durableLog;
    private final RaftMetadataStore metadataStore;
    private final KeyValueStateMachine stateMachine;
    private final ScheduledThreadPoolExecutor executor;
    private final List<RaftLogEntry> log = new ArrayList<>();
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private final Set<Integer> replicationInFlight = new HashSet<>();
    private final Map<Long, PendingProposal> proposals = new HashMap<>();
    private final Set<Long> durabilityCompletions = new HashSet<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private RaftRole role = RaftRole.FOLLOWER;
    private long currentTerm;
    private Integer votedFor;
    private Integer leaderId;
    private long commitIndex;
    private long lastApplied;
    private long durableIndex;
    private boolean healthy = true;
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;
    private volatile RaftStatus status;

    public RaftNode(
            RaftConfiguration configuration,
            RaftTransport transport,
            GroupCommitLog durableLog,
            RaftMetadataStore metadataStore,
            KeyValueStateMachine stateMachine) throws IOException {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.durableLog = Objects.requireNonNull(durableLog, "durableLog");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            var thread = new Thread(runnable, "raft-node-" + configuration.nodeId());
            thread.setUncaughtExceptionHandler((ignored, error) ->
                    LOGGER.error("Uncaught Raft executor failure", error));
            return thread;
        });
        this.executor.setRemoveOnCancelPolicy(true);

        log.addAll(durableLog.recover());
        validateRecoveredLog();
        durableIndex = lastLogIndex();
        var metadata = metadataStore.load();
        currentTerm = metadata.term();
        votedFor = parseVote(metadata.votedFor());
        if (metadata.commitIndex() > durableIndex) {
            throw new IOException("persisted commit index " + metadata.commitIndex()
                    + " is beyond recovered log index " + durableIndex);
        }
        commitIndex = metadata.commitIndex();
        applyCommitted();
        refreshStatus();
    }

    public void start() throws InterruptedException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("node is already started");
        }
        transport.start(this);
        execute(this::resetElectionTimer);
    }

    public CompletableFuture<Void> set(byte[] key, byte[] value) {
        return propose(RaftCommand.set(key, value)).thenApply(ignored -> null);
    }

    public CompletableFuture<Long> delete(List<byte[]> keys) {
        return propose(RaftCommand.deleteKeys(keys));
    }

    /** Performs a quorum-confirmed linearizable read on the leader. */
    public CompletableFuture<byte[]> get(byte[] key) {
        Objects.requireNonNull(key, "key");
        var keyCopy = key.clone();
        var result = new CompletableFuture<byte[]>();
        long deadline = System.nanoTime() + configuration.proposalTimeout().toNanos();
        execute(() -> attemptRead(keyCopy, result, deadline));
        return result;
    }

    public RaftStatus status() {
        return status;
    }

    public GroupCommitMetrics groupCommitMetrics() {
        return durableLog.metrics();
    }

    private CompletableFuture<Long> propose(RaftCommand command) {
        Objects.requireNonNull(command, "command");
        var result = new CompletableFuture<Long>();
        execute(() -> appendAsLeader(command, result));
        result.orTimeout(configuration.proposalTimeout().toMillis(), TimeUnit.MILLISECONDS);
        return result;
    }

    private void appendAsLeader(RaftCommand command, CompletableFuture<Long> clientFuture) {
        if (!healthy) {
            if (clientFuture != null) {
                clientFuture.completeExceptionally(new IllegalStateException("Raft storage is unhealthy"));
            }
            return;
        }
        if (role != RaftRole.LEADER) {
            if (clientFuture != null) {
                clientFuture.completeExceptionally(new NotLeaderException(leaderId));
            }
            return;
        }
        var entry = new RaftLogEntry(lastLogIndex() + 1, currentTerm, command);
        log.add(entry);
        if (clientFuture != null) {
            proposals.put(entry.index(), new PendingProposal(clientFuture));
        }
        refreshStatus();
        durableLog.append(entry).whenComplete((ignored, error) -> execute(() -> {
            if (error != null) {
                storageFailed(error);
                return;
            }
            durabilityCompletions.add(entry.index());
            advanceDurableIndex();
            matchIndex.put(configuration.nodeId(), durableIndex);
            advanceCommitIndex();
            replicateAll();
        }));
    }

    private void advanceDurableIndex() {
        while (durabilityCompletions.remove(durableIndex + 1)) {
            durableIndex++;
        }
    }

    private void attemptRead(byte[] key, CompletableFuture<byte[]> result, long deadlineNanos) {
        if (result.isDone()) {
            return;
        }
        if (!healthy) {
            result.completeExceptionally(new IllegalStateException("Raft storage is unhealthy"));
            return;
        }
        if (role != RaftRole.LEADER) {
            result.completeExceptionally(new NotLeaderException(leaderId));
            return;
        }
        if (System.nanoTime() >= deadlineNanos) {
            result.completeExceptionally(new QuorumUnavailableException("read quorum timed out"));
            return;
        }
        if (commitIndex == 0 || termAt(commitIndex) != currentTerm) {
            executor.schedule(() -> attemptRead(key, result, deadlineNanos), 5, TimeUnit.MILLISECONDS);
            return;
        }
        if (configuration.majority() == 1) {
            result.complete(stateMachine.get(key));
            return;
        }

        long readTerm = currentTerm;
        var acknowledgements = new int[] {1};
        var decided = new boolean[] {false};
        for (int peer : configuration.peers().keySet()) {
            var heartbeat = new RaftRpc.AppendEntriesRequest(readTerm, configuration.nodeId(),
                    0, 0, List.of(), commitIndex);
            transport.appendEntries(peer, heartbeat).whenComplete((response, error) -> execute(() -> {
                if (result.isDone() || decided[0] || role != RaftRole.LEADER || currentTerm != readTerm) {
                    return;
                }
                if (error != null) {
                    return;
                }
                if (response.term() > currentTerm) {
                    becomeFollower(response.term(), null);
                    result.completeExceptionally(new NotLeaderException(leaderId));
                    return;
                }
                if (response.term() == readTerm && ++acknowledgements[0] >= configuration.majority()) {
                    decided[0] = true;
                    result.complete(stateMachine.get(key));
                }
            }));
        }
        executor.schedule(() -> {
            if (!result.isDone() && !decided[0]) {
                attemptRead(key, result, deadlineNanos);
            }
        }, configuration.rpcTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<RaftRpc.RequestVoteResponse> requestVote(
            RaftRpc.RequestVoteRequest request) {
        var result = new CompletableFuture<RaftRpc.RequestVoteResponse>();
        execute(() -> handleRequestVote(request, result));
        return result;
    }

    private void handleRequestVote(
            RaftRpc.RequestVoteRequest request,
            CompletableFuture<RaftRpc.RequestVoteResponse> result) {
        if (!healthy) {
            result.completeExceptionally(new IllegalStateException("Raft storage is unhealthy"));
            return;
        }
        if (request.term() < currentTerm) {
            result.complete(new RaftRpc.RequestVoteResponse(currentTerm, false));
            return;
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), null);
        }
        boolean upToDate = request.lastLogTerm() > termAt(durableIndex)
                || (request.lastLogTerm() == termAt(durableIndex)
                && request.lastLogIndex() >= durableIndex);
        boolean canVote = votedFor == null || votedFor == request.candidateId();
        boolean granted = upToDate && canVote;
        if (granted) {
            votedFor = request.candidateId();
            persistMetadata();
            resetElectionTimer();
        }
        refreshStatus();
        result.complete(new RaftRpc.RequestVoteResponse(currentTerm, granted));
    }

    @Override
    public CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(
            RaftRpc.AppendEntriesRequest request) {
        var result = new CompletableFuture<RaftRpc.AppendEntriesResponse>();
        execute(() -> handleAppendEntries(request, result));
        return result;
    }

    private void handleAppendEntries(
            RaftRpc.AppendEntriesRequest request,
            CompletableFuture<RaftRpc.AppendEntriesResponse> result) {
        if (!healthy) {
            result.completeExceptionally(new IllegalStateException("Raft storage is unhealthy"));
            return;
        }
        if (request.term() < currentTerm) {
            result.complete(new RaftRpc.AppendEntriesResponse(
                    currentTerm, false, durableIndex, durableIndex + 1));
            return;
        }
        if (request.term() > currentTerm || role != RaftRole.FOLLOWER) {
            becomeFollower(request.term(), request.leaderId());
        }
        leaderId = request.leaderId();
        resetElectionTimer();

        if (request.prevLogIndex() > durableIndex) {
            result.complete(new RaftRpc.AppendEntriesResponse(
                    currentTerm, false, durableIndex, durableIndex + 1));
            return;
        }
        if (termAt(request.prevLogIndex()) != request.prevLogTerm()) {
            long conflict = firstIndexOfTerm(termAt(request.prevLogIndex()), request.prevLogIndex());
            result.complete(new RaftRpc.AppendEntriesResponse(
                    currentTerm, false, durableIndex, conflict));
            return;
        }

        int firstNew = 0;
        while (firstNew < request.entries().size()) {
            var incoming = request.entries().get(firstNew);
            if (incoming.index() > durableIndex) {
                break;
            }
            if (termAt(incoming.index()) != incoming.term()) {
                if (incoming.index() <= commitIndex) {
                    result.completeExceptionally(new IllegalStateException(
                            "leader attempted to replace committed index " + incoming.index()));
                    return;
                }
                int appendFrom = firstNew;
                durableLog.truncateSuffix(incoming.index()).whenComplete((ignored, error) ->
                        execute(() -> {
                            if (error != null) {
                                storageFailed(error);
                                result.completeExceptionally(unwrap(error));
                                return;
                            }
                            truncateInMemory(incoming.index());
                            appendFollowerEntries(request, appendFrom, result);
                        }));
                return;
            }
            firstNew++;
        }

        appendFollowerEntries(request, firstNew, result);
    }

    private void appendFollowerEntries(
            RaftRpc.AppendEntriesRequest request,
            int firstNew,
            CompletableFuture<RaftRpc.AppendEntriesResponse> result) {
        var additions = request.entries().subList(firstNew, request.entries().size());
        if (additions.isEmpty()) {
            advanceFollowerCommit(request.leaderCommit());
            long match = request.prevLogIndex() + request.entries().size();
            result.complete(new RaftRpc.AppendEntriesResponse(currentTerm, true, match, match + 1));
            return;
        }

        long expected = lastLogIndex() + 1;
        for (var entry : additions) {
            if (entry.index() != expected++) {
                result.completeExceptionally(new IllegalArgumentException("non-contiguous AppendEntries batch"));
                return;
            }
        }
        log.addAll(additions);
        refreshStatus();
        durableLog.append(additions).whenComplete((ignored, error) -> execute(() -> {
            if (error != null) {
                storageFailed(error);
                result.completeExceptionally(unwrap(error));
                return;
            }
            durableIndex = Math.max(durableIndex, additions.get(additions.size() - 1).index());
            advanceFollowerCommit(request.leaderCommit());
            long match = request.prevLogIndex() + request.entries().size();
            result.complete(new RaftRpc.AppendEntriesResponse(currentTerm, true, match, match + 1));
        }));
    }

    private void beginElection() {
        if (closed.get() || !healthy || role == RaftRole.LEADER) {
            return;
        }
        role = RaftRole.CANDIDATE;
        leaderId = null;
        currentTerm++;
        votedFor = configuration.nodeId();
        persistMetadata();
        resetElectionTimer();
        refreshStatus();

        long electionTerm = currentTerm;
        var votes = new int[] {1};
        if (votes[0] >= configuration.majority()) {
            becomeLeader();
            return;
        }
        var request = new RaftRpc.RequestVoteRequest(
                electionTerm, configuration.nodeId(), durableIndex, termAt(durableIndex));
        for (int peer : configuration.peers().keySet()) {
            transport.requestVote(peer, request).whenComplete((response, error) -> execute(() -> {
                if (error != null || role != RaftRole.CANDIDATE || currentTerm != electionTerm) {
                    return;
                }
                if (response.term() > currentTerm) {
                    becomeFollower(response.term(), null);
                } else if (response.voteGranted() && ++votes[0] >= configuration.majority()) {
                    becomeLeader();
                }
            }));
        }
    }

    private void becomeLeader() {
        if (role == RaftRole.LEADER) {
            return;
        }
        role = RaftRole.LEADER;
        leaderId = configuration.nodeId();
        cancel(electionTimer);
        nextIndex.clear();
        matchIndex.clear();
        replicationInFlight.clear();
        for (int member : configuration.members().keySet()) {
            nextIndex.put(member, durableIndex + 1);
            matchIndex.put(member, 0L);
        }
        matchIndex.put(configuration.nodeId(), durableIndex);
        refreshStatus();
        LOGGER.info("Node {} became leader for term {}", configuration.nodeId(), currentTerm);
        appendAsLeader(RaftCommand.noop(), null);
        cancel(heartbeatTimer);
        heartbeatTimer = executor.scheduleWithFixedDelay(
                this::replicateAll,
                0,
                configuration.heartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void becomeFollower(long term, Integer newLeaderId) {
        boolean newTerm = term > currentTerm;
        if (newTerm) {
            currentTerm = term;
            votedFor = null;
        }
        if (role != RaftRole.FOLLOWER) {
            var error = new NotLeaderException(newLeaderId);
            proposals.values().forEach(proposal -> proposal.future().completeExceptionally(error));
            proposals.clear();
        }
        role = RaftRole.FOLLOWER;
        leaderId = newLeaderId;
        cancel(heartbeatTimer);
        heartbeatTimer = null;
        replicationInFlight.clear();
        if (newTerm) {
            persistMetadata();
        }
        resetElectionTimer();
        refreshStatus();
    }

    private void replicateAll() {
        if (role != RaftRole.LEADER || !healthy) {
            return;
        }
        for (int peer : configuration.peers().keySet()) {
            replicateTo(peer);
        }
    }

    private void replicateTo(int peer) {
        if (role != RaftRole.LEADER || replicationInFlight.contains(peer)) {
            return;
        }
        long next = Math.max(1, Math.min(nextIndex.getOrDefault(peer, durableIndex + 1), durableIndex + 1));
        long lastToSend = Math.min(durableIndex, next + MAX_APPEND_BATCH - 1);
        var entries = next <= lastToSend
                ? List.copyOf(log.subList(toOffset(next), toOffset(lastToSend) + 1))
                : List.<RaftLogEntry>of();
        long requestTerm = currentTerm;
        var request = new RaftRpc.AppendEntriesRequest(
                requestTerm,
                configuration.nodeId(),
                next - 1,
                termAt(next - 1),
                entries,
                commitIndex);
        replicationInFlight.add(peer);
        transport.appendEntries(peer, request).whenComplete((response, error) -> execute(() -> {
            replicationInFlight.remove(peer);
            if (role != RaftRole.LEADER || currentTerm != requestTerm) {
                return;
            }
            if (error != null) {
                return;
            }
            if (response.term() > currentTerm) {
                becomeFollower(response.term(), null);
                return;
            }
            if (response.success()) {
                matchIndex.put(peer, response.matchIndex());
                nextIndex.put(peer, response.matchIndex() + 1);
                advanceCommitIndex();
                if (response.matchIndex() < durableIndex) {
                    replicateTo(peer);
                }
            } else {
                long fallback = Math.max(1, Math.min(next - 1, response.conflictIndex()));
                nextIndex.put(peer, fallback);
                replicateTo(peer);
            }
        }));
    }

    private void advanceCommitIndex() {
        if (role != RaftRole.LEADER) {
            return;
        }
        for (long candidate = durableIndex; candidate > commitIndex; candidate--) {
            if (termAt(candidate) != currentTerm) {
                continue;
            }
            int replicated = 1;
            for (int peer : configuration.peers().keySet()) {
                if (matchIndex.getOrDefault(peer, 0L) >= candidate) {
                    replicated++;
                }
            }
            if (replicated >= configuration.majority()) {
                commitTo(candidate);
                return;
            }
        }
    }

    private void advanceFollowerCommit(long leaderCommit) {
        long newCommit = Math.min(leaderCommit, durableIndex);
        if (newCommit > commitIndex) {
            commitTo(newCommit);
        }
    }

    private void commitTo(long newCommitIndex) {
        if (newCommitIndex <= commitIndex) {
            return;
        }
        commitIndex = newCommitIndex;
        persistMetadata();
        applyCommitted();
        refreshStatus();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            var entry = entryAt(++lastApplied);
            long affected = stateMachine.apply(entry.command());
            var proposal = proposals.remove(entry.index());
            if (proposal != null) {
                proposal.future().complete(affected);
            }
        }
    }

    private void resetElectionTimer() {
        if (closed.get() || role == RaftRole.LEADER) {
            return;
        }
        cancel(electionTimer);
        long minimum = configuration.electionTimeoutMin().toMillis();
        long maximum = configuration.electionTimeoutMax().toMillis();
        long delay = ThreadLocalRandom.current().nextLong(minimum, maximum);
        electionTimer = executor.schedule(this::beginElection, delay, TimeUnit.MILLISECONDS);
    }

    private void persistMetadata() {
        try {
            metadataStore.save(new RaftMetadata(
                    currentTerm, votedFor == null ? null : Integer.toString(votedFor), commitIndex));
        } catch (IOException error) {
            storageFailed(error);
        }
    }

    private void storageFailed(Throwable error) {
        var cause = unwrap(error);
        if (!healthy) {
            return;
        }
        healthy = false;
        role = RaftRole.FOLLOWER;
        leaderId = null;
        cancel(electionTimer);
        cancel(heartbeatTimer);
        proposals.values().forEach(proposal -> proposal.future().completeExceptionally(cause));
        proposals.clear();
        refreshStatus();
        LOGGER.error("Node {} stopped participating after a storage failure",
                configuration.nodeId(), cause);
    }

    private void truncateInMemory(long fromIndex) {
        int offset = toOffset(fromIndex);
        while (log.size() > offset) {
            var removed = log.remove(log.size() - 1);
            var proposal = proposals.remove(removed.index());
            if (proposal != null) {
                proposal.future().completeExceptionally(new NotLeaderException(leaderId));
            }
            durabilityCompletions.remove(removed.index());
        }
        durableIndex = Math.min(durableIndex, fromIndex - 1);
        refreshStatus();
    }

    private RaftLogEntry entryAt(long index) {
        if (index <= 0 || index > lastLogIndex()) {
            throw new IllegalArgumentException("log index out of range: " + index);
        }
        return log.get(toOffset(index));
    }

    private long termAt(long index) {
        return index == 0 ? 0 : entryAt(index).term();
    }

    private long lastLogIndex() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).index();
    }

    private int toOffset(long index) {
        return Math.toIntExact(index - 1);
    }

    private long firstIndexOfTerm(long term, long fromIndex) {
        long index = fromIndex;
        while (index > 1 && termAt(index - 1) == term) {
            index--;
        }
        return index;
    }

    private void validateRecoveredLog() throws IOException {
        long expected = 1;
        for (var entry : log) {
            if (entry.index() != expected++) {
                throw new IOException("Raft WAL is not contiguous at index " + entry.index());
            }
        }
    }

    private static Integer parseVote(String value) throws IOException {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException error) {
            throw new IOException("invalid persisted votedFor value: " + value, error);
        }
    }

    private void refreshStatus() {
        status = new RaftStatus(
                configuration.nodeId(), role, currentTerm, leaderId, commitIndex, lastApplied,
                lastLogIndex(), configuration.members().size());
    }

    private void execute(Runnable action) {
        if (closed.get()) {
            return;
        }
        executor.execute(() -> {
            try {
                action.run();
            } catch (RuntimeException error) {
                LOGGER.error("Raft state-machine action failed", error);
            }
        });
    }

    private static void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null
                ? error.getCause()
                : error;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        cancel(electionTimer);
        cancel(heartbeatTimer);
        var error = new IllegalStateException("Raft node closed");
        proposals.values().forEach(proposal -> proposal.future().completeExceptionally(error));
        proposals.clear();
        transport.close();
        executor.shutdownNow();
        try {
            durableLog.close();
        } catch (IOException closeError) {
            LOGGER.warn("Failed to close Raft WAL", closeError);
        }
    }

    private record PendingProposal(CompletableFuture<Long> future) {
    }
}
