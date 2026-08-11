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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
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
    private static final int MAX_APPEND_BYTES = 48 * 1024 * 1024;
    private static final int MAX_UNCOMMITTED_ENTRIES = 8_192;
    private static final int MAX_CLIENT_IN_FLIGHT = 8_192;

    private final RaftConfiguration configuration;
    private final RaftTransport transport;
    private final GroupCommitLog durableLog;
    private final RaftMetadataStore metadataStore;
    private final KeyValueStateMachine stateMachine;
    private final ScheduledThreadPoolExecutor executor;
    private final List<RaftLogEntry> log = new ArrayList<>();
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private final Map<Integer, Long> lastContactNanos = new HashMap<>();
    private final Map<Integer, Long> replicationInFlight = new HashMap<>();
    private final Map<Long, PendingProposal> proposals = new HashMap<>();
    private final List<PendingRead> readsAwaitingCurrentTermCommit = new ArrayList<>();
    private final Set<Long> durabilityCompletions = new HashSet<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Semaphore clientAdmission = new Semaphore(MAX_CLIENT_IN_FLIGHT);
    private final Set<CompletableFuture<?>> pendingOperations = ConcurrentHashMap.newKeySet();

    private RaftRole role = RaftRole.FOLLOWER;
    private long currentTerm;
    private Integer votedFor;
    private Integer leaderId;
    private long commitIndex;
    private long lastApplied;
    private long durableIndex;
    private boolean followerMutationInFlight;
    private boolean healthy = true;
    private long leadershipStartedNanos;
    private long replicationSequence;
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
        long recoveredLastTerm = termAt(durableIndex);
        if (metadata.term() < recoveredLastTerm) {
            throw new IOException("persisted term " + metadata.term()
                    + " is behind recovered log term " + recoveredLastTerm);
        }
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
        if (!execute(this::resetElectionTimer)) {
            throw new IllegalStateException("node was closed during startup");
        }
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
        if (!admitClientOperation(result)) {
            return result;
        }
        long deadline = System.nanoTime() + configuration.proposalTimeout().toNanos();
        submit(() -> attemptRead(keyCopy, result, deadline), result);
        result.orTimeout(configuration.proposalTimeout().toMillis(), TimeUnit.MILLISECONDS);
        result.whenComplete((ignored, error) -> {
            if (error != null) {
                execute(() -> readsAwaitingCurrentTermCommit.removeIf(
                        read -> read.future() == result));
            }
        });
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
        if (!admitClientOperation(result)) {
            return result;
        }
        submit(() -> appendAsLeader(command, result), result);
        result.orTimeout(configuration.proposalTimeout().toMillis(), TimeUnit.MILLISECONDS);
        result.whenComplete((ignored, error) -> {
            if (error != null) {
                execute(() -> proposals.entrySet().removeIf(
                        entry -> entry.getValue().future() == result));
            }
        });
        return result;
    }

    private boolean admitClientOperation(CompletableFuture<?> result) {
        if (!clientAdmission.tryAcquire()) {
            result.completeExceptionally(
                    new QuorumUnavailableException("too many client operations in flight"));
            return false;
        }
        result.whenComplete((ignored, error) -> clientAdmission.release());
        return true;
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
        if (clientFuture != null && lastLogIndex() - commitIndex >= MAX_UNCOMMITTED_ENTRIES) {
            clientFuture.completeExceptionally(
                    new QuorumUnavailableException("too many uncommitted entries"));
            return;
        }
        int commandBytes = command.encode().length;
        if (commandBytes + Long.BYTES * 2 + Integer.BYTES > MAX_APPEND_BYTES) {
            if (clientFuture != null) {
                clientFuture.completeExceptionally(
                        new IllegalArgumentException("command is too large to replicate"));
            } else {
                storageFailed(new IllegalArgumentException("internal command is too large to replicate"));
            }
            return;
        }
        var entry = new RaftLogEntry(lastLogIndex() + 1, currentTerm, command);
        var submission = durableLog.submitAppend(entry);
        if (!submission.accepted()) {
            handleRejectedAppend(command, clientFuture, submission);
            return;
        }
        log.add(entry);
        if (clientFuture != null) {
            proposals.put(entry.index(), new PendingProposal(clientFuture));
        }
        refreshStatus();
        submission.durability().whenComplete((ignored, error) -> execute(() -> {
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

    private void handleRejectedAppend(
            RaftCommand command,
            CompletableFuture<Long> clientFuture,
            GroupCommitLog.Submission submission) {
        if (submission.status() == GroupCommitLog.SubmissionStatus.OVERLOADED) {
            if (clientFuture != null) {
                clientFuture.completeExceptionally(
                        new QuorumUnavailableException("WAL admission queue is full"));
            } else if (command.type() == RaftCommand.Type.NOOP) {
                executor.schedule(
                        () -> appendAsLeader(command, null),
                        configuration.heartbeatInterval().toMillis(),
                        TimeUnit.MILLISECONDS);
            }
            return;
        }
        submission.durability().whenComplete((ignored, error) -> execute(() -> {
            var failure = error == null
                    ? new IllegalStateException("WAL is unavailable")
                    : unwrap(error);
            storageFailed(failure);
            if (clientFuture != null) {
                clientFuture.completeExceptionally(failure);
            }
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
            readsAwaitingCurrentTermCommit.add(new PendingRead(key, result, deadlineNanos));
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
                    0, 0, List.of(), 0);
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
        submit(() -> handleRequestVote(request, result), result);
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
            if (!healthy) {
                result.completeExceptionally(new IllegalStateException("could not persist the new term"));
                return;
            }
        }
        if (followerMutationInFlight) {
            result.complete(new RaftRpc.RequestVoteResponse(currentTerm, false));
            return;
        }
        boolean upToDate = request.lastLogTerm() > termAt(durableIndex)
                || (request.lastLogTerm() == termAt(durableIndex)
                && request.lastLogIndex() >= durableIndex);
        boolean canVote = votedFor == null || votedFor == request.candidateId();
        boolean granted = upToDate && canVote;
        if (granted) {
            votedFor = request.candidateId();
            if (!persistMetadata()) {
                result.complete(new RaftRpc.RequestVoteResponse(currentTerm, false));
                return;
            }
            resetElectionTimer();
        }
        refreshStatus();
        result.complete(new RaftRpc.RequestVoteResponse(currentTerm, granted));
    }

    @Override
    public CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(
            RaftRpc.AppendEntriesRequest request) {
        var result = new CompletableFuture<RaftRpc.AppendEntriesResponse>();
        submit(() -> handleAppendEntries(request, result), result);
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
            if (!healthy) {
                result.completeExceptionally(new IllegalStateException("could not persist the new term"));
                return;
            }
        }
        leaderId = request.leaderId();
        resetElectionTimer();

        if (followerMutationInFlight) {
            result.complete(new RaftRpc.AppendEntriesResponse(
                    currentTerm, false, durableIndex, durableIndex + 1));
            return;
        }

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
                var truncation = durableLog.submitTruncateSuffix(incoming.index());
                if (!truncation.accepted()) {
                    if (truncation.status() == GroupCommitLog.SubmissionStatus.OVERLOADED) {
                        result.completeExceptionally(
                                new QuorumUnavailableException("WAL admission queue is full"));
                    } else {
                        truncation.durability().whenComplete((ignored, error) -> execute(() -> {
                            var failure = error == null
                                    ? new IllegalStateException("WAL is unavailable")
                                    : unwrap(error);
                            storageFailed(failure);
                            result.completeExceptionally(failure);
                        }));
                    }
                    return;
                }
                beginFollowerMutation();
                truncation.durability().whenComplete((ignored, error) ->
                        execute(() -> {
                            if (error != null) {
                                storageFailed(error);
                                result.completeExceptionally(unwrap(error));
                                return;
                            }
                            truncateInMemory(incoming.index());
                            finishFollowerMutation();
                            if (!isCurrentLeaderRequest(request)) {
                                result.complete(new RaftRpc.AppendEntriesResponse(
                                        currentTerm, false, durableIndex, durableIndex + 1));
                                return;
                            }
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
            long match = request.prevLogIndex() + request.entries().size();
            advanceFollowerCommit(Math.min(request.leaderCommit(), match));
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
        var submission = durableLog.submitAppend(additions);
        if (!submission.accepted()) {
            if (submission.status() == GroupCommitLog.SubmissionStatus.OVERLOADED) {
                result.completeExceptionally(
                        new QuorumUnavailableException("WAL admission queue is full"));
            } else {
                submission.durability().whenComplete((ignored, error) -> execute(() -> {
                    var failure = error == null
                            ? new IllegalStateException("WAL is unavailable")
                            : unwrap(error);
                    storageFailed(failure);
                    result.completeExceptionally(failure);
                }));
            }
            return;
        }
        beginFollowerMutation();
        log.addAll(additions);
        refreshStatus();
        submission.durability().whenComplete((ignored, error) -> execute(() -> {
            if (error != null) {
                storageFailed(error);
                result.completeExceptionally(unwrap(error));
                return;
            }
            durableIndex = Math.max(durableIndex, additions.get(additions.size() - 1).index());
            finishFollowerMutation();
            if (!isCurrentLeaderRequest(request)) {
                result.complete(new RaftRpc.AppendEntriesResponse(
                        currentTerm, false, durableIndex, durableIndex + 1));
                return;
            }
            long match = request.prevLogIndex() + request.entries().size();
            advanceFollowerCommit(Math.min(request.leaderCommit(), match));
            result.complete(new RaftRpc.AppendEntriesResponse(currentTerm, true, match, match + 1));
        }));
    }

    private void beginElection() {
        if (closed.get() || !healthy || role == RaftRole.LEADER) {
            return;
        }
        if (followerMutationInFlight) {
            return;
        }
        role = RaftRole.CANDIDATE;
        leaderId = null;
        currentTerm++;
        votedFor = configuration.nodeId();
        if (!persistMetadata()) {
            return;
        }
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
        lastContactNanos.clear();
        replicationInFlight.clear();
        leadershipStartedNanos = System.nanoTime();
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
            failReadsAwaitingCurrentTermCommit(error);
        }
        role = RaftRole.FOLLOWER;
        leaderId = newLeaderId;
        cancel(heartbeatTimer);
        heartbeatTimer = null;
        replicationInFlight.clear();
        lastContactNanos.clear();
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
        if (!hasRecentQuorum()) {
            LOGGER.warn("Node {} stepped down after losing contact with a quorum in term {}",
                    configuration.nodeId(), currentTerm);
            becomeFollower(currentTerm, null);
            return;
        }
        for (int peer : configuration.peers().keySet()) {
            replicateTo(peer);
        }
    }

    private void replicateTo(int peer) {
        if (role != RaftRole.LEADER || replicationInFlight.containsKey(peer)) {
            return;
        }
        long next = Math.max(1, Math.min(nextIndex.getOrDefault(peer, durableIndex + 1), durableIndex + 1));
        var entries = replicationBatch(next);
        long requestTerm = currentTerm;
        var request = new RaftRpc.AppendEntriesRequest(
                requestTerm,
                configuration.nodeId(),
                next - 1,
                termAt(next - 1),
                entries,
                commitIndex);
        long replicationToken = ++replicationSequence;
        replicationInFlight.put(peer, replicationToken);
        transport.appendEntries(peer, request).whenComplete((response, error) -> execute(() -> {
            if (!replicationInFlight.remove(peer, replicationToken)) {
                return;
            }
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
            if (response.term() == currentTerm) {
                lastContactNanos.put(peer, System.nanoTime());
            }
            if (response.success()) {
                long confirmed = Math.max(
                        matchIndex.getOrDefault(peer, 0L),
                        Math.min(response.matchIndex(), durableIndex));
                matchIndex.put(peer, confirmed);
                nextIndex.merge(peer, confirmed + 1, Math::max);
                advanceCommitIndex();
                if (confirmed < durableIndex) {
                    replicateTo(peer);
                }
            } else {
                long fallback = Math.max(1, Math.min(next - 1, response.conflictIndex()));
                nextIndex.put(peer, fallback);
                executor.schedule(
                        () -> replicateTo(peer),
                        Math.min(25, configuration.heartbeatInterval().toMillis()),
                        TimeUnit.MILLISECONDS);
            }
        }));
    }

    private boolean hasRecentQuorum() {
        if (configuration.majority() == 1) {
            return true;
        }
        long now = System.nanoTime();
        long window = configuration.electionTimeoutMax().toNanos();
        if (now - leadershipStartedNanos < window) {
            return true;
        }
        int reachable = 1;
        long cutoff = now - window;
        for (int peer : configuration.peers().keySet()) {
            if (lastContactNanos.getOrDefault(peer, 0L) >= cutoff) {
                reachable++;
            }
        }
        return reachable >= configuration.majority();
    }

    private List<RaftLogEntry> replicationBatch(long firstIndex) {
        if (firstIndex > durableIndex) {
            return List.of();
        }
        var batch = new ArrayList<RaftLogEntry>(MAX_APPEND_BATCH);
        int encodedBytes = 0;
        for (long index = firstIndex;
             index <= durableIndex && batch.size() < MAX_APPEND_BATCH;
             index++) {
            var entry = entryAt(index);
            int entryBytes = Long.BYTES * 2 + Integer.BYTES + entry.command().encode().length;
            if (!batch.isEmpty() && encodedBytes + entryBytes > MAX_APPEND_BYTES) {
                break;
            }
            batch.add(entry);
            encodedBytes += entryBytes;
        }
        return List.copyOf(batch);
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
        if (!persistMetadata()) {
            return;
        }
        applyCommitted();
        refreshStatus();
        if (role == RaftRole.LEADER && termAt(commitIndex) == currentTerm) {
            resumeReadsAwaitingCurrentTermCommit();
        }
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
        if (closed.get() || role == RaftRole.LEADER || followerMutationInFlight) {
            return;
        }
        cancel(electionTimer);
        long minimum = configuration.electionTimeoutMin().toMillis();
        long maximum = configuration.electionTimeoutMax().toMillis();
        long delay = ThreadLocalRandom.current().nextLong(minimum, maximum);
        electionTimer = executor.schedule(this::beginElection, delay, TimeUnit.MILLISECONDS);
    }

    private void beginFollowerMutation() {
        followerMutationInFlight = true;
        cancel(electionTimer);
        electionTimer = null;
    }

    private void finishFollowerMutation() {
        followerMutationInFlight = false;
        resetElectionTimer();
    }

    private boolean isCurrentLeaderRequest(RaftRpc.AppendEntriesRequest request) {
        return healthy
                && role == RaftRole.FOLLOWER
                && currentTerm == request.term()
                && Objects.equals(leaderId, request.leaderId());
    }

    private boolean persistMetadata() {
        try {
            metadataStore.save(new RaftMetadata(
                    currentTerm, votedFor == null ? null : Integer.toString(votedFor), commitIndex));
            return true;
        } catch (IOException | RuntimeException error) {
            storageFailed(error);
            return false;
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
        failReadsAwaitingCurrentTermCommit(cause);
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

    private void resumeReadsAwaitingCurrentTermCommit() {
        if (readsAwaitingCurrentTermCommit.isEmpty()) {
            return;
        }
        var waiting = List.copyOf(readsAwaitingCurrentTermCommit);
        readsAwaitingCurrentTermCommit.clear();
        for (var read : waiting) {
            attemptRead(read.key(), read.future(), read.deadlineNanos());
        }
    }

    private void failReadsAwaitingCurrentTermCommit(Throwable error) {
        readsAwaitingCurrentTermCommit.forEach(
                read -> read.future().completeExceptionally(error));
        readsAwaitingCurrentTermCommit.clear();
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
                lastLogIndex(), configuration.members().size(), healthy);
    }

    private boolean execute(Runnable action) {
        if (closed.get()) {
            return false;
        }
        try {
            executor.execute(() -> {
                try {
                    action.run();
                } catch (RuntimeException error) {
                    LOGGER.error("Raft state-machine action failed", error);
                }
            });
            return true;
        } catch (RejectedExecutionException shuttingDown) {
            return false;
        }
    }

    private void submit(Runnable action, CompletableFuture<?> failureTarget) {
        pendingOperations.add(failureTarget);
        failureTarget.whenComplete((ignored, error) -> pendingOperations.remove(failureTarget));
        boolean accepted = execute(() -> {
            try {
                action.run();
            } catch (RuntimeException error) {
                failureTarget.completeExceptionally(error);
                LOGGER.error("Raft state-machine action failed", error);
            }
        });
        if (!accepted) {
            failureTarget.completeExceptionally(new IllegalStateException("Raft node is closed"));
        }
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
        var error = new IllegalStateException("Raft node closed");
        pendingOperations.forEach(operation -> operation.completeExceptionally(error));
        pendingOperations.clear();
        cancel(electionTimer);
        cancel(heartbeatTimer);
        transport.close();
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("Raft executor for node {} did not stop within 5 seconds",
                        configuration.nodeId());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        proposals.values().forEach(proposal -> proposal.future().completeExceptionally(error));
        proposals.clear();
        readsAwaitingCurrentTermCommit.clear();
        try {
            durableLog.close();
        } catch (IOException closeError) {
            LOGGER.warn("Failed to close Raft WAL", closeError);
        }
    }

    private record PendingProposal(CompletableFuture<Long> future) {
    }

    private record PendingRead(
            byte[] key, CompletableFuture<byte[]> future, long deadlineNanos) {
    }
}
