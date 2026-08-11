package io.github.khushpatel1234.raftkv.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Lifecycle owner for the Netty NIO RESP server and its bounded command executor. */
public final class RaftKvServer implements AutoCloseable {
    private static final int LISTEN_BACKLOG = 1_024;
    private static final long EVENT_LOOP_SHUTDOWN_SECONDS = 5L;

    private final ServerConfig config;
    private final CommandDispatcher dispatcher;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private BoundedCommandExecutor commandExecutor;
    private ChannelGroup clientChannels;
    private Channel serverChannel;

    public RaftKvServer(ServerConfig config, CommandDispatcher dispatcher) {
        this.config = Objects.requireNonNull(config, "config");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    }

    public RaftKvServer(int port, CommandDispatcher dispatcher) {
        this(ServerConfig.localhost(port), dispatcher);
    }

    /** Starts listening and returns the bound address. Port {@code 0} selects an ephemeral port. */
    public synchronized InetSocketAddress start() throws InterruptedException {
        if (serverChannel != null) {
            throw new IllegalStateException("server is already running");
        }

        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("raftkv-accept"));
        workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("raftkv-io"));
        commandExecutor = new BoundedCommandExecutor(
                config.commandThreads(), config.commandQueueCapacity());
        clientChannels = new DefaultChannelGroup(workerGroup.next(), true);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, LISTEN_BACKLOG)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.AUTO_READ, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(
                            ChannelOption.WRITE_BUFFER_WATER_MARK,
                            new WriteBufferWaterMark(
                                    config.writeBufferLowWatermarkBytes(),
                                    config.writeBufferHighWatermarkBytes()))
                    .childHandler(new RaftKvChannelInitializer(
                            config, dispatcher, commandExecutor, clientChannels));

            var bindFuture = bootstrap.bind(new InetSocketAddress(config.host(), config.port()));
            bindFuture.await();
            if (!bindFuture.isSuccess()) {
                throw new IllegalStateException(
                        "failed to bind RESP server to " + config.host() + ':' + config.port(),
                        bindFuture.cause());
            }
            serverChannel = bindFuture.channel();
            return (InetSocketAddress) serverChannel.localAddress();
        } catch (InterruptedException interrupted) {
            close();
            throw interrupted;
        } catch (RuntimeException startupFailure) {
            close();
            throw startupFailure;
        }
    }

    public synchronized boolean isRunning() {
        return serverChannel != null && serverChannel.isActive();
    }

    public synchronized InetSocketAddress localAddress() {
        if (serverChannel == null) {
            throw new IllegalStateException("server is not running");
        }
        return (InetSocketAddress) serverChannel.localAddress();
    }

    public synchronized int port() {
        return localAddress().getPort();
    }

    @Override
    public synchronized void close() {
        Channel channelToClose = serverChannel;
        serverChannel = null;
        if (channelToClose != null) {
            channelToClose.close().awaitUninterruptibly();
        }

        if (clientChannels != null) {
            for (Channel channel : clientChannels) {
                channel.config().setAutoRead(false);
            }
        }
        if (commandExecutor != null) {
            commandExecutor.close();
            commandExecutor = null;
        }
        if (clientChannels != null) {
            clientChannels.close().awaitUninterruptibly();
            clientChannels = null;
        }

        shutdownEventLoop(bossGroup);
        bossGroup = null;
        shutdownEventLoop(workerGroup);
        workerGroup = null;
    }

    private static void shutdownEventLoop(EventLoopGroup group) {
        if (group != null) {
            group.shutdownGracefully(0L, EVENT_LOOP_SHUTDOWN_SECONDS, TimeUnit.SECONDS)
                    .awaitUninterruptibly();
        }
    }
}
