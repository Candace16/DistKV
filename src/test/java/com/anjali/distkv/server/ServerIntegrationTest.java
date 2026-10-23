package com.anjali.distkv.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Server Integration Tests over TCP")
class ServerIntegrationTest {

    private Server server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        // Port 0 binds to a free random ephemeral port
        server = new Server(0);
        server.start();
        port = server.getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    @DisplayName("End-to-end command protocol execution over TCP socket")
    void testEndToEndProtocolFlow() throws Exception {
        try (Socket client = new Socket("127.0.0.1", port);
             BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8))) {

            // 1. PING -> PONG
            send(writer, "PING");
            assertEquals("PONG", reader.readLine());

            // 2. GET missing key -> NOT_FOUND
            send(writer, "GET user:1");
            assertEquals("NOT_FOUND", reader.readLine());

            // 3. SET user:1 anjali EX 60 -> OK
            send(writer, "SET user:1 anjali EX 60");
            assertEquals("OK", reader.readLine());

            // 4. GET user:1 -> VALUE anjali
            send(writer, "GET user:1");
            assertEquals("VALUE anjali", reader.readLine());

            // 5. EXISTS user:1 -> 1
            send(writer, "EXISTS user:1");
            assertEquals("1", reader.readLine());

            // 6. TTL user:1 -> seconds remaining (> 0)
            send(writer, "TTL user:1");
            long ttl = Long.parseLong(reader.readLine());
            assertTrue(ttl > 0 && ttl <= 60);

            // 7. STATS -> valid JSON payload
            send(writer, "STATS");
            String statsJson = reader.readLine();
            assertNotNull(statsJson);
            assertTrue(statsJson.contains("\"keys\":1"));
            assertTrue(statsJson.contains("\"node_status\":\"up\""));

            // 8. DEL user:1 -> OK
            send(writer, "DEL user:1");
            assertEquals("OK", reader.readLine());

            // 9. DEL again -> NOT_FOUND
            send(writer, "DEL user:1");
            assertEquals("NOT_FOUND", reader.readLine());

            // 10. Malformed command -> ERR
            send(writer, "INVALID_CMD a b c");
            String errResponse = reader.readLine();
            assertTrue(errResponse.startsWith("ERR "));
        }
    }

    private void send(BufferedWriter writer, String command) throws Exception {
        writer.write(command);
        writer.write("\r\n");
        writer.flush();
    }
}
