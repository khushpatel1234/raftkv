package io.github.khushpatel1234.raftkv.server;

import io.github.khushpatel1234.raftkv.resp.RespRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandParserTest {
    @Test
    void parsesPingAndEchoCaseInsensitively() throws Exception {
        Command.Ping ping = assertInstanceOf(Command.Ping.class, parse("pInG"));
        assertTrue(ping.message().isEmpty());

        Command.Ping pingWithMessage = assertInstanceOf(
                Command.Ping.class, parse("PING", "hello"));
        assertArrayEquals(bytes("hello"), pingWithMessage.message().orElseThrow());

        Command.Echo echo = assertInstanceOf(Command.Echo.class, parse("echo", "hello"));
        assertArrayEquals(bytes("hello"), echo.message());
    }

    @Test
    void parsesDataCommandsIncludingMultiKeyDel() throws Exception {
        Command.Get get = assertInstanceOf(Command.Get.class, parse("GET", "key"));
        assertArrayEquals(bytes("key"), get.key());

        Command.Set set = assertInstanceOf(Command.Set.class, parse("SET", "key", "value"));
        assertArrayEquals(bytes("key"), set.key());
        assertArrayEquals(bytes("value"), set.value());

        Command.Del del = assertInstanceOf(Command.Del.class, parse("DEL", "a", "b", "c"));
        assertEquals(3, del.keys().size());
        assertArrayEquals(bytes("a"), del.keys().get(0));
        assertArrayEquals(bytes("c"), del.keys().get(2));
    }

    @Test
    void infoAndRaftInfoAreAliasesAndGroupStatsIsRecognized() throws Exception {
        assertInstanceOf(Command.Info.class, parse("INFO"));
        assertInstanceOf(Command.Info.class, parse("RAFT.INFO"));
        assertInstanceOf(Command.GroupStats.class, parse("GROUP.STATS"));
    }

    @Test
    void commandsDefensivelyCopyBinaryValues() {
        byte[] sourceKey = new byte[] {1, 2};
        byte[] sourceValue = new byte[] {3, 4};
        Command.Set command = new Command.Set(sourceKey, sourceValue);
        sourceKey[0] = 9;
        sourceValue[0] = 9;
        assertArrayEquals(new byte[] {1, 2}, command.key());
        assertArrayEquals(new byte[] {3, 4}, command.value());

        byte[] returnedKey = command.key();
        returnedKey[0] = 8;
        assertArrayEquals(new byte[] {1, 2}, command.key());

        byte[] listKey = new byte[] {5};
        Command.Del del = new Command.Del(List.of(listKey));
        listKey[0] = 6;
        List<byte[]> returnedKeys = del.keys();
        returnedKeys.get(0)[0] = 7;
        assertArrayEquals(new byte[] {5}, del.keys().get(0));
    }

    @Test
    void rejectsWrongArityForEveryCommandShape() {
        assertArityError("PING", "a", "b");
        assertArityError("ECHO");
        assertArityError("GET");
        assertArityError("SET", "key");
        assertArityError("DEL");
        assertArityError("INFO", "extra");
        assertArityError("RAFT.INFO", "extra");
        assertArityError("GROUP.STATS", "extra");
    }

    @Test
    void rejectsEmptyUnknownAndOversizedCommandNamesSafely() {
        CommandParseException empty = assertThrows(
                CommandParseException.class,
                () -> CommandParser.parse(new RespRequest(List.of())));
        assertEquals("ERR empty command", empty.getMessage());

        CommandParseException unknown = assertThrows(
                CommandParseException.class,
                () -> CommandParser.parse(new RespRequest(List.of(new byte[] {1, 'X'}))));
        assertEquals("ERR unknown command '?x'", unknown.getMessage());

        CommandParseException tooLong = assertThrows(
                CommandParseException.class,
                () -> CommandParser.parse(new RespRequest(List.of(new byte[65]))));
        assertEquals("ERR command name is too long", tooLong.getMessage());
    }

    private static Command parse(String... arguments) throws CommandParseException {
        return CommandParser.parse(request(arguments));
    }

    private static RespRequest request(String... arguments) {
        return new RespRequest(java.util.Arrays.stream(arguments).map(CommandParserTest::bytes).toList());
    }

    private static void assertArityError(String... arguments) {
        CommandParseException failure = assertThrows(
                CommandParseException.class, () -> CommandParser.parse(request(arguments)));
        assertTrue(failure.getMessage().startsWith("ERR wrong number of arguments"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
