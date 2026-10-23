package com.anjali.distkv.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Parses client text lines into strongly-typed {@link Command} objects.
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Never throws an exception on bad input; always returns {@link Command.Invalid}.</li>
 *   <li>Enforces {@link #MAX_LINE_LENGTH} limit to protect against OOM memory exhaustion.</li>
 *   <li>Command verbs are case-insensitive.</li>
 * </ul>
 */
public final class CommandParser {

    public static final int MAX_LINE_LENGTH = 8192; // 8 KB safety limit

    private CommandParser() {}

    /**
     * Parses a single line of input received over TCP into a {@link Command}.
     *
     * @param rawLine the raw string sent by the client (excluding trailing \r\n)
     * @return the parsed Command, or {@link Command.Invalid} if the syntax is incorrect
     */
    public static Command parse(String rawLine) {
        if (rawLine == null) {
            return new Command.Invalid("Empty command");
        }

        if (rawLine.startsWith("\uFEFF")) {
            rawLine = rawLine.substring(1);
        }

        if (rawLine.length() > MAX_LINE_LENGTH) {
            return new Command.Invalid("Line length exceeds maximum limit of " + MAX_LINE_LENGTH + " characters");
        }

        String trimmed = rawLine.trim();
        if (trimmed.isEmpty()) {
            return new Command.Invalid("Empty command");
        }

        List<String> tokens = tokenize(trimmed);
        if (tokens.isEmpty()) {
            return new Command.Invalid("Empty command");
        }

        String verb = tokens.get(0).toUpperCase();

        return switch (verb) {
            case "PING" -> parsePing(tokens);
            case "GET" -> parseGet(tokens);
            case "SET" -> parseSet(tokens);
            case "DEL" -> parseDel(tokens);
            case "EXISTS" -> parseExists(tokens);
            case "TTL" -> parseTtl(tokens);
            case "STATS" -> parseStats(tokens);
            default -> new Command.Invalid("Unknown command '" + tokens.get(0) + "'");
        };
    }

    private static Command parsePing(List<String> tokens) {
        if (tokens.size() > 1) {
            return new Command.Invalid("Syntax: PING takes no arguments");
        }
        return new Command.Ping();
    }

    private static Command parseGet(List<String> tokens) {
        if (tokens.size() != 2) {
            return new Command.Invalid("Syntax: GET <key>");
        }
        return new Command.Get(tokens.get(1));
    }

    private static Command parseDel(List<String> tokens) {
        if (tokens.size() != 2) {
            return new Command.Invalid("Syntax: DEL <key>");
        }
        return new Command.Del(tokens.get(1));
    }

    private static Command parseExists(List<String> tokens) {
        if (tokens.size() != 2) {
            return new Command.Invalid("Syntax: EXISTS <key>");
        }
        return new Command.Exists(tokens.get(1));
    }

    private static Command parseTtl(List<String> tokens) {
        if (tokens.size() != 2) {
            return new Command.Invalid("Syntax: TTL <key>");
        }
        return new Command.Ttl(tokens.get(1));
    }

    private static Command parseStats(List<String> tokens) {
        if (tokens.size() > 1) {
            return new Command.Invalid("Syntax: STATS takes no arguments");
        }
        return new Command.Stats();
    }

    private static Command parseSet(List<String> tokens) {
        // Syntax: SET key value [EX seconds]
        if (tokens.size() == 3) {
            return new Command.Set(tokens.get(1), tokens.get(2), OptionalLong.empty());
        }

        if (tokens.size() == 5) {
            String exFlag = tokens.get(3).toUpperCase();
            if (!exFlag.equals("EX")) {
                return new Command.Invalid("Syntax error: expected 'EX <seconds>' but got '" + tokens.get(3) + "'");
            }
            try {
                long seconds = Long.parseLong(tokens.get(4));
                if (seconds <= 0) {
                    return new Command.Invalid("Invalid expire time in SET: must be positive integer");
                }
                return new Command.Set(tokens.get(1), tokens.get(2), OptionalLong.of(seconds));
            } catch (NumberFormatException e) {
                return new Command.Invalid("Invalid expire time in SET: not a valid number");
            }
        }

        return new Command.Invalid("Syntax: SET <key> <value> [EX <seconds>]");
    }

    /**
     * Splits string on whitespace while ignoring consecutive whitespace.
     */
    private static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }
}
