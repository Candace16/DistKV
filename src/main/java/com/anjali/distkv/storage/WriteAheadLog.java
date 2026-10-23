package com.anjali.distkv.storage;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sequential Write-Ahead Log (WAL) for durability.
 *
 * <p>All state-mutating commands (SET, DEL) are appended to disk before being
 * applied in memory. Supports three configurable fsync policies:
 * <ul>
 *   <li>{@link FsyncPolicy#ALWAYS}: fsync on every write (highest safety, lowest throughput).</li>
 *   <li>{@link FsyncPolicy#EVERY_SECOND}: periodic background fsync (near-zero overhead, max 1s loss).</li>
 *   <li>{@link FsyncPolicy#NEVER}: relies purely on OS page cache writeback.</li>
 * </ul>
 */
public final class WriteAheadLog implements Closeable {

    public enum FsyncPolicy {
        ALWAYS,
        EVERY_SECOND,
        NEVER;

        public static FsyncPolicy fromString(String val) {
            if (val == null) return EVERY_SECOND;
            return switch (val.toLowerCase().trim()) {
                case "always", "everywrite" -> ALWAYS;
                case "never" -> NEVER;
                default -> EVERY_SECOND;
            };
        }
    }

    private final File logFile;
    private final FsyncPolicy fsyncPolicy;
    private final ReentrantLock lock = new ReentrantLock();

    private FileOutputStream fos;
    private FileChannel channel;
    private ScheduledExecutorService syncScheduler;

    public WriteAheadLog(File logFile, FsyncPolicy fsyncPolicy) throws IOException {
        this.logFile = logFile;
        this.fsyncPolicy = fsyncPolicy;

        if (logFile.getParentFile() != null && !logFile.getParentFile().exists()) {
            logFile.getParentFile().mkdirs();
        }

        this.fos = new FileOutputStream(logFile, true); // Append mode
        this.channel = fos.getChannel();

        if (fsyncPolicy == FsyncPolicy.EVERY_SECOND) {
            this.syncScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "distkv-wal-fsync");
                t.setDaemon(true);
                return t;
            });
            this.syncScheduler.scheduleWithFixedDelay(this::syncSafe, 1, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * Appends a SET operation to the log.
     * Record format: SET <key> <value> <expireAtMillis>\n
     */
    public void appendSet(String key, String value, long expireAtMillis) throws IOException {
        String record = "SET " + key + " " + value + " " + expireAtMillis + "\n";
        appendRaw(record);
    }

    /**
     * Appends a DEL operation to the log.
     * Record format: DEL <key>\n
     */
    public void appendDel(String key) throws IOException {
        String record = "DEL " + key + "\n";
        appendRaw(record);
    }

    private void appendRaw(String record) throws IOException {
        byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        lock.lock();
        try {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            if (fsyncPolicy == FsyncPolicy.ALWAYS) {
                channel.force(false);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Flushes channel buffers and forces an fsync to physical storage.
     */
    public void sync() throws IOException {
        lock.lock();
        try {
            if (channel != null && channel.isOpen()) {
                channel.force(false);
            }
        } finally {
            lock.unlock();
        }
    }

    private void syncSafe() {
        try {
            sync();
        } catch (IOException ignored) {}
    }

    /**
     * Truncates the log to 0 bytes (used after a successful snapshot).
     */
    public void truncate() throws IOException {
        lock.lock();
        try {
            syncSafe();
            channel.truncate(0);
            channel.position(0);
            if (fsyncPolicy == FsyncPolicy.ALWAYS) {
                channel.force(false);
            }
        } finally {
            lock.unlock();
        }
    }

    public File getLogFile() {
        return logFile;
    }

    public FsyncPolicy getFsyncPolicy() {
        return fsyncPolicy;
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (syncScheduler != null) {
                syncScheduler.shutdownNow();
            }
            if (channel != null && channel.isOpen()) {
                channel.force(false);
                channel.close();
            }
            if (fos != null) {
                fos.close();
            }
        } finally {
            lock.unlock();
        }
    }
}
