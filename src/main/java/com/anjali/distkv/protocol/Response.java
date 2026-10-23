package com.anjali.distkv.protocol;

/**
 * Standard protocol response constants and formatting helpers.
 */
public final class Response {

    public static final String OK = "OK";
    public static final String PONG = "PONG";
    public static final String NOT_FOUND = "NOT_FOUND";
    public static final String EXISTS_TRUE = "1";
    public static final String EXISTS_FALSE = "0";

    private Response() {}

    public static String value(String value) {
        return "VALUE " + value;
    }

    public static String err(String message) {
        return "ERR " + message;
    }

    public static String ttl(long seconds) {
        return String.valueOf(seconds);
    }
}
