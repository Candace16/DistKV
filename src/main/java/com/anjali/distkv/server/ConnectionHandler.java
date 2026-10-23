package com.anjali.distkv.server;

import com.anjali.distkv.protocol.Command;
import com.anjali.distkv.protocol.CommandParser;
import com.anjali.distkv.protocol.Response;
import com.anjali.distkv.store.Store;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

/**
 * Handles communication with a single TCP client connection.
 *
 * <p>Key responsibilities:
 * <ul>
 *   <li>Enforces {@link CommandParser#MAX_LINE_LENGTH} at the byte-stream level to prevent OOM DoS attacks.</li>
 *   <li>Catches socket timeouts, IO errors, and client disconnects gracefully.</li>
 *   <li>Executes parsed commands against the provided {@link Store}.</li>
 * </ul>
 */
public final class ConnectionHandler implements Runnable {

    private static final int DEFAULT_SO_TIMEOUT_MS = 60_000; // 60 seconds idle timeout
    private final Socket socket;
    private final Store store;
    private final com.anjali.distkv.storage.WriteAheadLog wal;
    private final com.anjali.distkv.cluster.ClusterRouter router;

    public ConnectionHandler(Socket socket, Store store) {
        this(socket, store, null, null);
    }

    public ConnectionHandler(Socket socket, Store store, com.anjali.distkv.storage.WriteAheadLog wal) {
        this(socket, store, wal, null);
    }

    public ConnectionHandler(Socket socket, Store store, com.anjali.distkv.storage.WriteAheadLog wal, com.anjali.distkv.cluster.ClusterRouter router) {
        this.socket = socket;
        this.store = store;
        this.wal = wal;
        this.router = router;
    }

    @Override
    public void run() {
        try (socket) {
            socket.setSoTimeout(DEFAULT_SO_TIMEOUT_MS);
            InputStream in = socket.getInputStream();
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

            while (!socket.isClosed()) {
                String line;
                try {
                    line = readSafeLine(in, CommandParser.MAX_LINE_LENGTH);
                } catch (EOFException e) {
                    // Normal client disconnect
                    break;
                } catch (SocketTimeoutException e) {
                    // Idle connection closed
                    break;
                } catch (LineTooLongException e) {
                    out.write(Response.err("Line exceeds maximum length of " + CommandParser.MAX_LINE_LENGTH + " bytes"));
                    out.write("\r\n");
                    out.flush();
                    continue;
                }

                if (line == null) {
                    break;
                }

                Command command = CommandParser.parse(line);
                String responseText = execute(command);

                out.write(responseText);
                out.write("\r\n");
                out.flush();
            }
        } catch (IOException e) {
            // Client abruptly disconnected or reset connection; ignore and cleanup cleanly
        }
    }

    /**
     * Executes the given command against the store and returns the protocol response.
     */
    public String execute(Command command) {
        if (router != null) {
            return router.execute(command);
        }

        if (command instanceof Command.Ping) {
            return Response.PONG;
        } else if (command instanceof Command.Set set) {
            if (wal != null) {
                try {
                    long expireAt = set.ttlSeconds().isPresent()
                            ? System.currentTimeMillis() + (set.ttlSeconds().getAsLong() * 1000L)
                            : com.anjali.distkv.store.SimpleStore.StoredItem.NO_EXPIRY;
                    wal.appendSet(set.key(), set.value(), expireAt);
                } catch (IOException e) {
                    return Response.err("WAL append failure: " + e.getMessage());
                }
            }
            store.set(set.key(), set.value(), set.ttlSeconds());
            return Response.OK;
        } else if (command instanceof Command.Get get) {
            String val = store.get(get.key());
            return (val != null) ? Response.value(val) : Response.NOT_FOUND;
        } else if (command instanceof Command.Del del) {
            if (wal != null) {
                try {
                    wal.appendDel(del.key());
                } catch (IOException e) {
                    return Response.err("WAL append failure: " + e.getMessage());
                }
            }
            boolean deleted = store.del(del.key());
            return deleted ? Response.OK : Response.NOT_FOUND;
        } else if (command instanceof Command.Exists exists) {
            boolean ex = store.exists(exists.key());
            return ex ? Response.EXISTS_TRUE : Response.EXISTS_FALSE;
        } else if (command instanceof Command.Ttl ttl) {
            return Response.ttl(store.ttl(ttl.key()));
        } else if (command instanceof Command.Stats) {
            return store.statsJson();
        } else if (command instanceof Command.Invalid invalid) {
            return Response.err(invalid.reason());
        } else {
            return Response.err("Unknown command");
        }
    }

    /**
     * Reads a single line terminated by '\n' (with optional preceding '\r') up to maxBytes.
     * Throws {@link LineTooLongException} if maxBytes is exceeded before a newline is encountered.
     */
    private static String readSafeLine(InputStream in, int maxBytes) throws IOException, LineTooLongException {
        StringBuilder sb = new StringBuilder();
        int bytesRead = 0;

        while (true) {
            int b = in.read();
            if (b == -1) {
                if (sb.length() == 0) {
                    throw new EOFException();
                }
                break;
            }

            bytesRead++;
            if (bytesRead > maxBytes) {
                // Drain the rest of the overflowing line up to '\n' so the stream remains in sync
                drainToNewline(in);
                throw new LineTooLongException();
            }

            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }

    private static void drainToNewline(InputStream in) throws IOException {
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                break;
            }
        }
    }

    private static final class LineTooLongException extends Exception {}
}
