package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespRequestDecoder;
import io.github.khushpatel1234.raftkv.resp.RespResponseEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;

import java.util.Objects;
import java.util.concurrent.Executor;

/** Installs the RESP transport pipeline on each accepted connection. */
public final class RaftKvChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ServerConfig config;
    private final CommandDispatcher dispatcher;
    private final Executor commandExecutor;
    private final ChannelGroup clientChannels;

    public RaftKvChannelInitializer(
            ServerConfig config,
            CommandDispatcher dispatcher,
            Executor commandExecutor,
            ChannelGroup clientChannels) {
        this.config = Objects.requireNonNull(config, "config");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor");
        this.clientChannels = Objects.requireNonNull(clientChannels, "clientChannels");
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        clientChannels.add(channel);
        channel.pipeline()
                .addLast("respDecoder", new RespRequestDecoder(
                        config.maxArguments(), config.maxBulkStringBytes(), config.maxFrameBytes()))
                .addLast("respEncoder", new RespResponseEncoder())
                .addLast("commands", new CommandExecutionHandler(
                        dispatcher,
                        commandExecutor,
                        config.pendingResponseLowWatermark(),
                        config.pendingResponseHighWatermark()));
    }
}
