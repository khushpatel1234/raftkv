package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespResponse;

import java.util.concurrent.CompletionStage;

/**
 * Asynchronous boundary between the RESP transport and the replicated state machine.
 * Implementations should return protocol-ready responses and must not block the caller.
 */
@FunctionalInterface
public interface CommandDispatcher {
    CompletionStage<RespResponse> dispatch(Command command);
}
