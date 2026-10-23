package com.anjali.distkv.protocol;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CommandParser Unit Tests")
class CommandParserTest {

    @Test
    @DisplayName("Parse PING command with case insensitivity")
    void testParsePing() {
        assertInstanceOf(Command.Ping.class, CommandParser.parse("PING"));
        assertInstanceOf(Command.Ping.class, CommandParser.parse("ping"));
        assertInstanceOf(Command.Ping.class, CommandParser.parse("  PiNg   "));

        // With extra arguments
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("PING extra"));
    }

    @Test
    @DisplayName("Parse SET command with and without EX ttl")
    void testParseSet() {
        Command cmd1 = CommandParser.parse("SET user:1 anjali");
        assertInstanceOf(Command.Set.class, cmd1);
        Command.Set set1 = (Command.Set) cmd1;
        assertEquals("user:1", set1.key());
        assertEquals("anjali", set1.value());
        assertTrue(set1.ttlSeconds().isEmpty());

        Command cmd2 = CommandParser.parse("set user:2 rahul ex 30");
        assertInstanceOf(Command.Set.class, cmd2);
        Command.Set set2 = (Command.Set) cmd2;
        assertEquals("user:2", set2.key());
        assertEquals("rahul", set2.value());
        assertEquals(OptionalLong.of(30), set2.ttlSeconds());
    }

    @Test
    @DisplayName("Parse SET with invalid TTL should return Invalid command")
    void testParseSetInvalidTtl() {
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("SET k v EX -5"));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("SET k v EX 0"));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("SET k v EX not_a_num"));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("SET k v WRONG 10"));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("SET k"));
    }

    @Test
    @DisplayName("Parse GET, DEL, EXISTS, TTL commands")
    void testParseSingleArgCommands() {
        Command getCmd = CommandParser.parse("GET mykey");
        assertInstanceOf(Command.Get.class, getCmd);
        assertEquals("mykey", ((Command.Get) getCmd).key());

        Command delCmd = CommandParser.parse("del mykey");
        assertInstanceOf(Command.Del.class, delCmd);
        assertEquals("mykey", ((Command.Del) delCmd).key());

        Command existsCmd = CommandParser.parse("EXISTS mykey");
        assertInstanceOf(Command.Exists.class, existsCmd);
        assertEquals("mykey", ((Command.Exists) existsCmd).key());

        Command ttlCmd = CommandParser.parse("TTL mykey");
        assertInstanceOf(Command.Ttl.class, ttlCmd);
        assertEquals("mykey", ((Command.Ttl) ttlCmd).key());
    }

    @Test
    @DisplayName("Parse STATS command")
    void testParseStats() {
        assertInstanceOf(Command.Stats.class, CommandParser.parse("STATS"));
        assertInstanceOf(Command.Stats.class, CommandParser.parse("stats"));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("STATS extra_arg"));
    }

    @Test
    @DisplayName("Empty, blank, and null lines")
    void testEmptyAndBlankLines() {
        assertInstanceOf(Command.Invalid.class, CommandParser.parse(null));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse(""));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("   "));
        assertInstanceOf(Command.Invalid.class, CommandParser.parse("\t\n"));
    }

    @Test
    @DisplayName("Unknown command verbs return Invalid command")
    void testUnknownCommand() {
        Command cmd = CommandParser.parse("FOOBAR a b c");
        assertInstanceOf(Command.Invalid.class, cmd);
        Command.Invalid inv = (Command.Invalid) cmd;
        assertTrue(inv.reason().contains("FOOBAR"));
    }

    @Test
    @DisplayName("Line exceeding MAX_LINE_LENGTH returns Invalid")
    void testLineTooLong() {
        String longKey = "a".repeat(CommandParser.MAX_LINE_LENGTH + 10);
        Command cmd = CommandParser.parse("GET " + longKey);
        assertInstanceOf(Command.Invalid.class, cmd);
        assertTrue(((Command.Invalid) cmd).reason().contains("maximum limit"));
    }
}
