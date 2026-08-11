package io.github.khushpatel1234.raftkv;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

record NodeOptions(
        int nodeId,
        String clientHost,
        int clientPort,
        String raftHost,
        int raftPort,
        Map<Integer, InetSocketAddress> members,
        Path dataDirectory,
        int workerThreads,
        int groupCommitBatch,
        Duration groupCommitDelay) {

    private static final String DEFAULT_PEERS = "1=127.0.0.1:7000";

    static NodeOptions parse(String[] args) {
        var values = new LinkedHashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            var argument = args[i];
            if ("--help".equals(argument) || "-h".equals(argument)) {
                throw new HelpRequested();
            }
            if (!argument.startsWith("--")) {
                throw new IllegalArgumentException("expected an option, got: " + argument);
            }
            String name;
            String value;
            int equals = argument.indexOf('=');
            if (equals > 2) {
                name = argument.substring(2, equals);
                value = argument.substring(equals + 1);
            } else {
                name = argument.substring(2);
                if (++i >= args.length) {
                    throw new IllegalArgumentException("missing value for --" + name);
                }
                value = args[i];
            }
            if (values.put(name, value) != null) {
                throw new IllegalArgumentException("duplicate option --" + name);
            }
        }

        rejectUnknown(values);
        int nodeId = positiveInt(values.getOrDefault("node-id", "1"), "node-id");
        int clientPort = port(values.getOrDefault("client-port", "6379"), "client-port");
        int raftPort = port(values.getOrDefault("raft-port", "7000"), "raft-port");
        int workers = positiveInt(values.getOrDefault("worker-threads", "16"), "worker-threads");
        int batch = positiveInt(values.getOrDefault("group-commit-batch", "64"), "group-commit-batch");
        int delayMillis = nonNegativeInt(
                values.getOrDefault("group-commit-delay-ms", "2"), "group-commit-delay-ms");
        var members = parseMembers(values.getOrDefault("peers", DEFAULT_PEERS));
        if (!members.containsKey(nodeId)) {
            throw new IllegalArgumentException("--peers must include local --node-id " + nodeId);
        }
        return new NodeOptions(
                nodeId,
                values.getOrDefault("client-host", "0.0.0.0"),
                clientPort,
                values.getOrDefault("raft-host", "0.0.0.0"),
                raftPort,
                members,
                Path.of(values.getOrDefault("data-dir", "data/node-" + nodeId)),
                workers,
                batch,
                Duration.ofMillis(delayMillis));
    }

    static String usage() {
        return """
                Usage: java -jar target/raftkv.jar [options]

                  --node-id N                   Local member id (default: 1)
                  --client-host HOST            RESP bind host (default: 0.0.0.0)
                  --client-port PORT            RESP port (default: 6379)
                  --raft-host HOST              Internal Raft bind host (default: 0.0.0.0)
                  --raft-port PORT              Internal Raft port (default: 7000)
                  --peers MEMBERS               Full membership, e.g.
                                                1=host1:7000,2=host2:7000,3=host3:7000
                  --data-dir PATH               WAL/metadata directory (default: data/node-N)
                  --worker-threads N            Command workers (default: 16)
                  --group-commit-batch N        Maximum WAL batch (default: 64)
                  --group-commit-delay-ms N     Maximum batching delay (default: 2)
                  --help                        Show this help
                """;
    }

    private static Map<Integer, InetSocketAddress> parseMembers(String value) {
        var members = new LinkedHashMap<Integer, InetSocketAddress>();
        if (value.isBlank()) {
            throw new IllegalArgumentException("--peers cannot be empty");
        }
        for (var rawMember : value.split(",")) {
            var member = rawMember.trim();
            int equals = member.indexOf('=');
            int colon = member.lastIndexOf(':');
            if (equals <= 0 || colon <= equals + 1 || colon == member.length() - 1) {
                throw new IllegalArgumentException("invalid peer '" + member + "' (expected id=host:port)");
            }
            int id = positiveInt(member.substring(0, equals), "peer id");
            var host = member.substring(equals + 1, colon);
            if (host.startsWith("[") && host.endsWith("]")) {
                host = host.substring(1, host.length() - 1);
            }
            int peerPort = port(member.substring(colon + 1), "peer port");
            if (members.put(id, InetSocketAddress.createUnresolved(host, peerPort)) != null) {
                throw new IllegalArgumentException("duplicate peer id " + id);
            }
        }
        return Map.copyOf(members);
    }

    private static void rejectUnknown(Map<String, String> values) {
        var known = java.util.Set.of(
                "node-id", "client-host", "client-port", "raft-host", "raft-port", "peers",
                "data-dir", "worker-threads", "group-commit-batch", "group-commit-delay-ms");
        values.keySet().stream().filter(key -> !known.contains(key)).findFirst()
                .ifPresent(key -> {
                    throw new IllegalArgumentException("unknown option --" + key);
                });
    }

    private static int port(String value, String name) {
        int result = positiveInt(value, name);
        if (result > 65_535) {
            throw new IllegalArgumentException(name + " must be <= 65535");
        }
        return result;
    }

    private static int positiveInt(String value, String name) {
        int result = nonNegativeInt(value, name);
        if (result == 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return result;
    }

    private static int nonNegativeInt(String value, String name) {
        try {
            int result = Integer.parseInt(value);
            if (result < 0) {
                throw new IllegalArgumentException(name + " cannot be negative");
            }
            return result;
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " must be an integer: " + value, error);
        }
    }

    static final class HelpRequested extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
