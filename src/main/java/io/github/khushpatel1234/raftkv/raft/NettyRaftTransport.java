package io.github.khushpatel1234.raftkv.raft;

import io.github.khushpatel1234.raftkv.core.RaftCommand;
import io.github.khushpatel1234.raftkv.core.RaftLogEntry;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Persistent, multiplexed Netty transport for Raft's two RPCs.
 *
 * <p>The wire format is deliberately small and private to the cluster. Each frame carries a
 * request id, allowing election traffic, replication, and read barriers to share a connection.
 * Client RESP traffic uses a separate port and pipeline.</p>
 */
public final class NettyRaftTransport implements RaftTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRaftTransport.class);
    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private final int localNodeId;
    private final InetSocketAddress bindAddress;
    private final Map<Integer, InetSocketAddress> peers;
    private final Duration rpcTimeout;
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup ioGroup = new NioEventLoopGroup(2);
    private final AtomicLong requestIds = new AtomicLong();
    private final Map<Long, CompletableFuture<WireMessage>> pending = new ConcurrentHashMap<>();
    private final Map<Integer, PeerConnection> connections = new ConcurrentHashMap<>();
    private volatile RaftRpcHandler handler;
    private volatile Channel serverChannel;
    private volatile boolean closed;

    public NettyRaftTransport(
            int localNodeId,
            InetSocketAddress bindAddress,
            Map<Integer, InetSocketAddress> members,
            Duration rpcTimeout) {
        this.localNodeId = localNodeId;
        this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
        this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout");
        var peerCopy = new HashMap<>(Objects.requireNonNull(members, "members"));
        peerCopy.remove(localNodeId);
        this.peers = Map.copyOf(peerCopy);
        for (var entry : peers.entrySet()) {
            connections.put(entry.getKey(), new PeerConnection(entry.getValue()));
        }
    }

    @Override
    public void start(RaftRpcHandler rpcHandler) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("transport is closed");
        }
        if (handler != null) {
            throw new IllegalStateException("transport is already started");
        }
        handler = Objects.requireNonNull(rpcHandler, "rpcHandler");
        var bootstrap = new ServerBootstrap()
                .group(bossGroup, ioGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new PipelineInitializer());
        serverChannel = bootstrap.bind(bindAddress).sync().channel();
        LOGGER.info("Raft transport for node {} listening on {}", localNodeId, bindAddress);
    }

    @Override
    public CompletableFuture<RaftRpc.RequestVoteResponse> requestVote(
            int targetNodeId, RaftRpc.RequestVoteRequest request) {
        var id = requestIds.incrementAndGet();
        return send(targetNodeId, new VoteRequest(id, request), VoteResponse.class)
                .thenApply(VoteResponse::response);
    }

    @Override
    public CompletableFuture<RaftRpc.AppendEntriesResponse> appendEntries(
            int targetNodeId, RaftRpc.AppendEntriesRequest request) {
        var id = requestIds.incrementAndGet();
        return send(targetNodeId, new AppendRequest(id, request), AppendResponse.class)
                .thenApply(AppendResponse::response);
    }

    private <T extends WireMessage> CompletableFuture<T> send(
            int targetNodeId, WireMessage request, Class<T> responseType) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("transport is closed"));
        }
        var peer = connections.get(targetNodeId);
        if (peer == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("unknown Raft node " + targetNodeId));
        }

        var raw = new CompletableFuture<WireMessage>();
        pending.put(request.requestId(), raw);
        raw.orTimeout(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((ignored, error) -> pending.remove(request.requestId(), raw));
        peer.channel().whenComplete((channel, connectError) -> {
            if (connectError != null) {
                raw.completeExceptionally(connectError);
                return;
            }
            if (raw.isDone()) {
                peer.invalidate(channel);
                return;
            }
            raw.whenComplete((ignored, error) -> {
                if (error != null) {
                    peer.invalidate(channel);
                }
            });
            if (!channel.isActive() || !channel.isWritable()) {
                raw.completeExceptionally(
                        new IllegalStateException("Raft peer channel is not writable"));
                return;
            }
            channel.writeAndFlush(request).addListener(write -> {
                if (!write.isSuccess()) {
                    raw.completeExceptionally(write.cause());
                    peer.invalidate(channel);
                }
            });
        });
        return raw.thenApply(message -> {
            if (!responseType.isInstance(message)) {
                throw new IllegalStateException("unexpected Raft response " + message.getClass());
            }
            return responseType.cast(message);
        });
    }

    private void receive(WireMessage message, Channel channel) {
        if (message instanceof VoteResponse || message instanceof AppendResponse) {
            var future = pending.remove(message.requestId());
            if (future != null) {
                future.complete(message);
            }
            return;
        }
        var rpcHandler = handler;
        if (rpcHandler == null) {
            channel.close();
            return;
        }
        if (message instanceof VoteRequest vote) {
            rpcHandler.requestVote(vote.request()).whenComplete((response, error) -> {
                if (error != null) {
                    LOGGER.warn("RequestVote handler failed", error);
                    channel.close();
                } else {
                    channel.writeAndFlush(new VoteResponse(vote.requestId(), response));
                }
            });
        } else if (message instanceof AppendRequest append) {
            rpcHandler.appendEntries(append.request()).whenComplete((response, error) -> {
                if (error != null) {
                    LOGGER.warn("AppendEntries handler failed", error);
                    channel.close();
                } else {
                    channel.writeAndFlush(new AppendResponse(append.requestId(), response));
                }
            });
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        var error = new IllegalStateException("transport closed");
        pending.values().forEach(future -> future.completeExceptionally(error));
        pending.clear();
        connections.values().forEach(PeerConnection::close);
        var channel = serverChannel;
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
    }

    private final class PeerConnection {
        private final InetSocketAddress address;
        private final AtomicReference<CompletableFuture<Channel>> current = new AtomicReference<>();

        private PeerConnection(InetSocketAddress address) {
            this.address = address;
        }

        synchronized CompletableFuture<Channel> channel() {
            if (closed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("transport is closed"));
            }
            var existing = current.get();
            if (existing != null) {
                if (!existing.isDone()) {
                    return existing;
                }
                Channel channel;
                try {
                    channel = existing.getNow(null);
                } catch (java.util.concurrent.CompletionException failedConnection) {
                    channel = null;
                }
                if (channel != null && channel.isActive()) {
                    return existing;
                }
                current.compareAndSet(existing, null);
                if (channel != null) {
                    channel.close();
                }
            }
            var connecting = connect();
            current.set(connecting);
            connecting.whenComplete((channel, error) -> {
                if (error != null) {
                    current.compareAndSet(connecting, null);
                }
            });
            return connecting;
        }

        private CompletableFuture<Channel> connect() {
            var result = new CompletableFuture<Channel>();
            var bootstrap = new Bootstrap()
                    .group(ioGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) rpcTimeout.toMillis())
                    .handler(new PipelineInitializer());
            bootstrap.connect(address).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    result.complete(future.channel());
                } else {
                    result.completeExceptionally(future.cause());
                }
            });
            return result;
        }

        void invalidate(Channel channel) {
            var existing = current.get();
            if (existing != null) {
                Channel active = null;
                try {
                    active = existing.getNow(null);
                } catch (java.util.concurrent.CompletionException failedConnection) {
                    // A later failed connection must not prevent this old channel from closing.
                }
                if (active == channel) {
                    current.compareAndSet(existing, null);
                }
            }
            channel.close();
        }

        synchronized void close() {
            var existing = current.getAndSet(null);
            if (existing != null) {
                existing.thenAccept(Channel::close);
            }
        }
    }

    private final class PipelineInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel channel) {
            channel.pipeline()
                    .addLast(new IdleStateHandler(0, 0, 60))
                    .addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void userEventTriggered(ChannelHandlerContext context, Object event)
                                throws Exception {
                            if (event instanceof IdleStateEvent) {
                                context.close();
                            } else {
                                super.userEventTriggered(context, event);
                            }
                        }
                    })
                    .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_BYTES, 0, 4, 0, 4))
                    .addLast(new LengthFieldPrepender(4))
                    .addLast(new WireCodec())
                    .addLast(new SimpleChannelInboundHandler<WireMessage>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext context, WireMessage message) {
                            receive(message, context.channel());
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                            LOGGER.warn("Closing Raft channel after a protocol or transport error", cause);
                            context.close();
                        }
                    });
        }
    }

    private sealed interface WireMessage
            permits VoteRequest, VoteResponse, AppendRequest, AppendResponse {
        long requestId();
    }

    private record VoteRequest(long requestId, RaftRpc.RequestVoteRequest request)
            implements WireMessage {
    }

    private record VoteResponse(long requestId, RaftRpc.RequestVoteResponse response)
            implements WireMessage {
    }

    private record AppendRequest(long requestId, RaftRpc.AppendEntriesRequest request)
            implements WireMessage {
    }

    private record AppendResponse(long requestId, RaftRpc.AppendEntriesResponse response)
            implements WireMessage {
    }

    private static final class WireCodec extends ByteToMessageCodec<WireMessage> {
        private static final int VOTE_REQUEST = 1;
        private static final int VOTE_RESPONSE = 2;
        private static final int APPEND_REQUEST = 3;
        private static final int APPEND_RESPONSE = 4;

        @Override
        protected void encode(ChannelHandlerContext context, WireMessage message, ByteBuf out) {
            if (message instanceof VoteRequest vote) {
                out.writeByte(VOTE_REQUEST).writeLong(vote.requestId());
                var request = vote.request();
                out.writeLong(request.term()).writeInt(request.candidateId())
                        .writeLong(request.lastLogIndex()).writeLong(request.lastLogTerm());
            } else if (message instanceof VoteResponse vote) {
                out.writeByte(VOTE_RESPONSE).writeLong(vote.requestId());
                out.writeLong(vote.response().term()).writeBoolean(vote.response().voteGranted());
            } else if (message instanceof AppendRequest append) {
                out.writeByte(APPEND_REQUEST).writeLong(append.requestId());
                var request = append.request();
                out.writeLong(request.term()).writeInt(request.leaderId())
                        .writeLong(request.prevLogIndex()).writeLong(request.prevLogTerm())
                        .writeLong(request.leaderCommit()).writeInt(request.entries().size());
                for (var entry : request.entries()) {
                    var command = entry.command().encode();
                    out.writeLong(entry.index()).writeLong(entry.term()).writeInt(command.length)
                            .writeBytes(command);
                }
            } else if (message instanceof AppendResponse append) {
                out.writeByte(APPEND_RESPONSE).writeLong(append.requestId());
                var response = append.response();
                out.writeLong(response.term()).writeBoolean(response.success())
                        .writeLong(response.matchIndex()).writeLong(response.conflictIndex());
            } else {
                throw new IllegalArgumentException("unsupported Raft wire message " + message);
            }
        }

        @Override
        protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out) {
            var type = in.readUnsignedByte();
            var requestId = in.readLong();
            switch (type) {
                case VOTE_REQUEST -> out.add(new VoteRequest(requestId, new RaftRpc.RequestVoteRequest(
                        in.readLong(), in.readInt(), in.readLong(), in.readLong())));
                case VOTE_RESPONSE -> out.add(new VoteResponse(requestId, new RaftRpc.RequestVoteResponse(
                        in.readLong(), in.readBoolean())));
                case APPEND_REQUEST -> out.add(decodeAppendRequest(requestId, in));
                case APPEND_RESPONSE -> out.add(new AppendResponse(requestId, new RaftRpc.AppendEntriesResponse(
                        in.readLong(), in.readBoolean(), in.readLong(), in.readLong())));
                default -> throw new IllegalArgumentException("unknown Raft message type " + type);
            }
        }

        private static AppendRequest decodeAppendRequest(long requestId, ByteBuf in) {
            var term = in.readLong();
            var leaderId = in.readInt();
            var prevLogIndex = in.readLong();
            var prevLogTerm = in.readLong();
            var leaderCommit = in.readLong();
            var count = in.readInt();
            if (count < 0 || count > 1_024) {
                throw new IllegalArgumentException("invalid AppendEntries count " + count);
            }
            var entries = new java.util.ArrayList<RaftLogEntry>(count);
            for (int i = 0; i < count; i++) {
                var index = in.readLong();
                var entryTerm = in.readLong();
                var commandLength = in.readInt();
                if (commandLength < 0 || commandLength > in.readableBytes()) {
                    throw new IllegalArgumentException("invalid Raft command length " + commandLength);
                }
                var encoded = new byte[commandLength];
                in.readBytes(encoded);
                entries.add(new RaftLogEntry(index, entryTerm, RaftCommand.decode(encoded)));
            }
            return new AppendRequest(requestId, new RaftRpc.AppendEntriesRequest(
                    term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit));
        }
    }
}
