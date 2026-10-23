package com.anjali.distkv.protocol;

import java.util.OptionalLong;

/**
 * Represents a parsed command received from a client.
 *
 * <p>Implemented as a sealed hierarchy to ensure pattern matching completeness
 * and strict type-safety across the system.
 */
public sealed interface Command {

    record Set(String key, String value, OptionalLong ttlSeconds) implements Command {}

    record Get(String key) implements Command {}

    record Del(String key) implements Command {}

    record Exists(String key) implements Command {}

    record Ttl(String key) implements Command {}

    record Ping() implements Command {}

    record Stats() implements Command {}

    record Invalid(String reason) implements Command {}
}
