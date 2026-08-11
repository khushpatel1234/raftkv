package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespRequest;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Validates command arity and turns raw RESP arguments into typed commands. */
public final class CommandParser {
    private static final int MAX_COMMAND_NAME_BYTES = 64;

    private CommandParser() {
    }

    public static Command parse(RespRequest request) throws CommandParseException {
        if (request.size() == 0) {
            throw new CommandParseException("ERR empty command");
        }

        byte[] rawName = request.argument(0);
        if (rawName.length == 0) {
            throw new CommandParseException("ERR empty command name");
        }
        if (rawName.length > MAX_COMMAND_NAME_BYTES) {
            throw new CommandParseException("ERR command name is too long");
        }

        String name = new String(rawName, StandardCharsets.US_ASCII).toUpperCase(Locale.ROOT);
        return switch (name) {
            case "PING" -> parsePing(request);
            case "ECHO" -> {
                requireArity(request, 2, "echo");
                yield new Command.Echo(request.argument(1));
            }
            case "GET" -> {
                requireArity(request, 2, "get");
                yield new Command.Get(request.argument(1));
            }
            case "SET" -> {
                requireArity(request, 3, "set");
                yield new Command.Set(request.argument(1), request.argument(2));
            }
            case "DEL" -> {
                requireMinimumArity(request, 2, "del");
                List<byte[]> keys = request.arguments().subList(1, request.size());
                yield new Command.Del(keys);
            }
            case "INFO", "RAFT.INFO" -> {
                requireArity(request, 1, name.toLowerCase(Locale.ROOT));
                yield new Command.Info();
            }
            case "GROUP.STATS" -> {
                requireArity(request, 1, "group.stats");
                yield new Command.GroupStats();
            }
            default -> throw new CommandParseException(
                    "ERR unknown command '" + printableCommandName(rawName) + "'");
        };
    }

    private static Command parsePing(RespRequest request) throws CommandParseException {
        if (request.size() == 1) {
            return new Command.Ping();
        }
        if (request.size() == 2) {
            return new Command.Ping(request.argument(1));
        }
        throw wrongArity("ping");
    }

    private static void requireArity(RespRequest request, int expected, String name)
            throws CommandParseException {
        if (request.size() != expected) {
            throw wrongArity(name);
        }
    }

    private static void requireMinimumArity(RespRequest request, int minimum, String name)
            throws CommandParseException {
        if (request.size() < minimum) {
            throw wrongArity(name);
        }
    }

    private static CommandParseException wrongArity(String commandName) {
        return new CommandParseException(
                "ERR wrong number of arguments for '" + commandName + "' command");
    }

    private static String printableCommandName(byte[] rawName) {
        byte[] sanitized = Arrays.copyOf(rawName, rawName.length);
        for (int index = 0; index < sanitized.length; index++) {
            int value = Byte.toUnsignedInt(sanitized[index]);
            if (value < 0x20 || value > 0x7e) {
                sanitized[index] = '?';
            }
        }
        return new String(sanitized, StandardCharsets.US_ASCII).toLowerCase(Locale.ROOT);
    }
}
