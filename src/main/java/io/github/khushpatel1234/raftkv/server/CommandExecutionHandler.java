package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespRequest;
import io.github.khushpatel1234.raftkv.resp.RespResponse;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Offloads commands to a bounded executor and serializes asynchronous completions in request order.
 * It also pauses socket reads at explicit pending-response and channel-writability watermarks.
 */
public final class CommandExecutionHandler extends SimpleChannelInboundHandler<RespRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandExecutionHandler.class);

    private final CommandDispatcher dispatcher;
    private final Executor commandExecutor;
    private final int pendingLowWatermark;
    private final int pendingHighWatermark;
    private final Deque<PendingResponse> pendingResponses = new ArrayDeque<>();

    private boolean pausedForPendingResponses;
    private boolean pausedForChannelWrites;

    public CommandExecutionHandler(
            CommandDispatcher dispatcher,
            Executor commandExecutor,
            int pendingLowWatermark,
            int pendingHighWatermark) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor");
        if (pendingLowWatermark < 0 || pendingHighWatermark <= pendingLowWatermark) {
            throw new IllegalArgumentException("pending watermarks must satisfy 0 <= low < high");
        }
        this.pendingLowWatermark = pendingLowWatermark;
        this.pendingHighWatermark = pendingHighWatermark;
    }

    @Override
    public void channelActive(ChannelHandlerContext context) throws Exception {
        pausedForChannelWrites = !context.channel().isWritable();
        applyAutoRead(context);
        super.channelActive(context);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, RespRequest request) {
        PendingResponse pending = new PendingResponse();
        pendingResponses.addLast(pending);
        updatePendingBackpressure(context);

        final Command command;
        try {
            command = CommandParser.parse(request);
        } catch (CommandParseException parseFailure) {
            completeOnEventLoop(context, pending, RespResponse.error(parseFailure.getMessage()), null);
            return;
        }

        try {
            commandExecutor.execute(() -> invokeCommand(context, pending, command));
        } catch (RejectedExecutionException saturated) {
            completeOnEventLoop(
                    context, pending, RespResponse.error("BUSY command queue is full"), null);
        }
    }

    private void invokeCommand(ChannelHandlerContext context, PendingResponse pending, Command command) {
        final CompletionStage<RespResponse> responseStage;
        try {
            responseStage = localResponse(command);
        } catch (RuntimeException dispatchFailure) {
            completeFromAnyThread(context, pending, null, dispatchFailure);
            return;
        }

        if (responseStage == null) {
            completeFromAnyThread(
                    context, pending, null, new NullPointerException("dispatcher returned null"));
            return;
        }
        try {
            responseStage.whenComplete(
                    (response, failure) -> completeFromAnyThread(context, pending, response, failure));
        } catch (RuntimeException callbackFailure) {
            completeFromAnyThread(context, pending, null, callbackFailure);
        }
    }

    private CompletionStage<RespResponse> localResponse(Command command) {
        return switch (command) {
            case Command.Ping ping -> CompletableFuture.completedFuture(
                    ping.message()
                            .<RespResponse>map(RespResponse::bulk)
                            .orElseGet(() -> RespResponse.simple("PONG")));
            case Command.Echo echo -> CompletableFuture.completedFuture(RespResponse.bulk(echo.message()));
            default -> dispatcher.dispatch(command);
        };
    }

    private void completeFromAnyThread(
            ChannelHandlerContext context,
            PendingResponse pending,
            RespResponse response,
            Throwable failure) {
        Runnable completion = () -> completeOnEventLoop(context, pending, response, failure);
        if (context.executor().inEventLoop()) {
            completion.run();
            return;
        }
        try {
            context.executor().execute(completion);
        } catch (RejectedExecutionException shuttingDown) {
            LOGGER.debug("Discarding command completion while channel event loop is shutting down");
        }
    }

    private void completeOnEventLoop(
            ChannelHandlerContext context,
            PendingResponse pending,
            RespResponse response,
            Throwable failure) {
        if (pending.completed) {
            return;
        }
        pending.response = failure == null && response != null
                ? response
                : RespResponse.error("ERR internal server error");
        pending.completed = true;
        if (failure != null) {
            LOGGER.warn("Command execution failed", failure);
        } else if (response == null) {
            LOGGER.warn("Command dispatcher completed with a null response");
        }
        drainCompletedResponses(context);
    }

    private void drainCompletedResponses(ChannelHandlerContext context) {
        boolean wroteResponse = false;
        while (!pendingResponses.isEmpty() && pendingResponses.peekFirst().completed) {
            PendingResponse pending = pendingResponses.removeFirst();
            if (context.channel().isActive()) {
                context.write(pending.response);
                wroteResponse = true;
            }
        }
        if (wroteResponse) {
            context.flush();
        }
        updatePendingBackpressure(context);
    }

    private void updatePendingBackpressure(ChannelHandlerContext context) {
        int pendingCount = pendingResponses.size();
        if (!pausedForPendingResponses && pendingCount >= pendingHighWatermark) {
            pausedForPendingResponses = true;
        } else if (pausedForPendingResponses && pendingCount <= pendingLowWatermark) {
            pausedForPendingResponses = false;
        }
        applyAutoRead(context);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext context) throws Exception {
        pausedForChannelWrites = !context.channel().isWritable();
        applyAutoRead(context);
        context.fireChannelWritabilityChanged();
    }

    private void applyAutoRead(ChannelHandlerContext context) {
        boolean shouldRead = !pausedForPendingResponses && !pausedForChannelWrites;
        if (context.channel().config().isAutoRead() != shouldRead) {
            context.channel().config().setAutoRead(shouldRead);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        pendingResponses.clear();
        super.channelInactive(context);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        if (cause instanceof TooLongFrameException) {
            context.writeAndFlush(RespResponse.error("ERR request is too large"))
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }
        if (cause instanceof CorruptedFrameException) {
            context.writeAndFlush(RespResponse.error("ERR Protocol error"))
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }
        LOGGER.warn("Closing RESP connection after an unexpected pipeline error", cause);
        context.close();
    }

    int pendingResponseCount() {
        return pendingResponses.size();
    }

    private static final class PendingResponse {
        private RespResponse response;
        private boolean completed;
    }
}
