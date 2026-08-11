package io.github.khushpatel1234.raftkv;

import io.github.khushpatel1234.raftkv.core.KeyValueStateMachine;
import io.github.khushpatel1234.raftkv.raft.NettyRaftTransport;
import io.github.khushpatel1234.raftkv.raft.RaftConfiguration;
import io.github.khushpatel1234.raftkv.raft.RaftNode;
import io.github.khushpatel1234.raftkv.server.RaftKvServer;
import io.github.khushpatel1234.raftkv.server.ServerConfig;
import io.github.khushpatel1234.raftkv.storage.GroupCommitLog;
import io.github.khushpatel1234.raftkv.storage.RaftMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        final NodeOptions options;
        try {
            options = NodeOptions.parse(args);
        } catch (NodeOptions.HelpRequested help) {
            System.out.print(NodeOptions.usage());
            return;
        } catch (IllegalArgumentException invalid) {
            System.err.println("error: " + invalid.getMessage());
            System.err.println();
            System.err.print(NodeOptions.usage());
            System.exit(2);
            return;
        }

        Files.createDirectories(options.dataDirectory());
        var raftConfiguration = RaftConfiguration.defaults(options.nodeId(), options.members());
        var transport = new NettyRaftTransport(
                options.nodeId(),
                new InetSocketAddress(options.raftHost(), options.raftPort()),
                options.members(),
                raftConfiguration.rpcTimeout());
        var log = new GroupCommitLog(
                options.dataDirectory().resolve("raft.wal"),
                options.groupCommitBatch(),
                options.groupCommitDelay());
        var metadata = new RaftMetadataStore(options.dataDirectory().resolve("raft.meta"));
        var node = new RaftNode(
                raftConfiguration, transport, log, metadata, new KeyValueStateMachine());

        var defaults = ServerConfig.defaults(options.clientHost(), options.clientPort());
        var serverConfig = new ServerConfig(
                defaults.host(),
                defaults.port(),
                options.workerThreads(),
                defaults.commandQueueCapacity(),
                defaults.pendingResponseLowWatermark(),
                defaults.pendingResponseHighWatermark(),
                defaults.writeBufferLowWatermarkBytes(),
                defaults.writeBufferHighWatermarkBytes(),
                defaults.maxArguments(),
                defaults.maxBulkStringBytes(),
                defaults.maxFrameBytes());
        var server = new RaftKvServer(serverConfig, new RaftCommandDispatcher(node));
        var stopped = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Stopping RaftKV node {}", options.nodeId());
            server.close();
            node.close();
            stopped.countDown();
        }, "raftkv-shutdown"));

        try {
            node.start();
            var clientAddress = server.start();
            LOGGER.info("RaftKV node {} accepting RESP on {} and Raft on {}:{}",
                    options.nodeId(), clientAddress, options.raftHost(), options.raftPort());
            stopped.await();
        } catch (Throwable startupFailure) {
            server.close();
            node.close();
            throw startupFailure;
        }
    }
}
