package com.anjali.distkv.cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

/**
 * Low-latency TCP client used for inter-node replication and fallback reads.
 */
public final class ReplicationClient {

    private static final int CONNECT_TIMEOUT_MS = 500;
    private static final int READ_TIMEOUT_MS = 1000;

    /**
     * Sends a raw command line over TCP to a target node and returns the single-line response.
     *
     * @param host       target host or IP
     * @param port       target TCP port
     * @param rawCommand formatted command (e.g. "SET k v")
     * @return response string, or {@code null} on failure
     */
    public String sendCommand(String host, int port, String rawCommand) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            writer.write(rawCommand);
            writer.write("\r\n");
            writer.flush();

            return reader.readLine();
        } catch (IOException e) {
            return null; // Target node unreachable or timed out
        }
    }

    public boolean ping(String host, int port) {
        String resp = sendCommand(host, port, "PING");
        return "PONG".equalsIgnoreCase(resp);
    }

    public boolean replicateSet(String host, int port, String key, String value, OptionalLong ttlSeconds) {
        String cmd = ttlSeconds.isPresent()
                ? "SET " + key + " " + value + " EX " + ttlSeconds.getAsLong()
                : "SET " + key + " " + value;
        String resp = sendCommand(host, port, cmd);
        return "OK".equalsIgnoreCase(resp);
    }

    public boolean replicateDel(String host, int port, String key) {
        String resp = sendCommand(host, port, "DEL " + key);
        return "OK".equalsIgnoreCase(resp) || "NOT_FOUND".equalsIgnoreCase(resp);
    }

    public String fallbackGet(String host, int port, String key) {
        String resp = sendCommand(host, port, "GET " + key);
        if (resp != null && resp.startsWith("VALUE ")) {
            return resp.substring(6);
        }
        return null;
    }
}
