package com.anjali.distkv.storage;

import com.anjali.distkv.store.SimpleStore;
import com.anjali.distkv.store.Store;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages point-in-time snapshots of the in-memory store.
 *
 * <p>Snapshot Safety:
 * <ul>
 *   <li>Writes to a temporary file ({@code snapshot.tmp}) first and forces an fsync.</li>
 *   <li>Atomically renames {@code snapshot.tmp} to {@code snapshot.db} via {@link StandardCopyOption#ATOMIC_MOVE}.
 *       If the system crashes during writing, the existing valid snapshot is never corrupted.</li>
 *   <li>Truncates the {@link WriteAheadLog} upon successful completion to bound disk usage.</li>
 * </ul>
 */
public final class SnapshotManager implements Closeable {

    public static final String SNAPSHOT_FILE_NAME = "snapshot.db";
    public static final String TEMP_FILE_NAME = "snapshot.tmp";

    private final File dataDir;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean snapshotInProgress = new AtomicBoolean(false);

    public SnapshotManager(File dataDir) {
        this(dataDir, 0); // 0 = manual only
    }

    public SnapshotManager(File dataDir, long intervalSeconds) {
        this.dataDir = dataDir;
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        if (intervalSeconds > 0) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "distkv-snapshotter");
                t.setDaemon(true);
                return t;
            });
        } else {
            this.scheduler = null;
        }
    }

    /**
     * Starts periodic snapshots if a scheduler was configured.
     */
    public void startPeriodic(Store store, WriteAheadLog wal, long intervalSeconds) {
        if (scheduler != null && intervalSeconds > 0) {
            scheduler.scheduleWithFixedDelay(
                    () -> {
                        try {
                            takeSnapshot(store, wal);
                        } catch (IOException e) {
                            System.err.println("[DistKV] Snapshot failed: " + e.getMessage());
                        }
                    },
                    intervalSeconds,
                    intervalSeconds,
                    TimeUnit.SECONDS
            );
        }
    }

    /**
     * Executes a point-in-time snapshot with atomic swapping and log truncation.
     *
     * @param store the active store to dump
     * @param wal   the write-ahead log to truncate upon success (may be null)
     * @return the written snapshot file
     */
    public File takeSnapshot(Store store, WriteAheadLog wal) throws IOException {
        if (!snapshotInProgress.compareAndSet(false, true)) {
            throw new IllegalStateException("Snapshot already in progress");
        }

        File tempFile = new File(dataDir, TEMP_FILE_NAME);
        File targetFile = new File(dataDir, SNAPSHOT_FILE_NAME);

        try {
            // 1. Write store state to temporary file
            try (FileOutputStream fos = new FileOutputStream(tempFile);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {

                store.dump((key, item) -> {
                    try {
                        writer.write(key + " " + item.value() + " " + item.expireAtMillis() + "\n");
                    } catch (IOException e) {
                        throw new RuntimeException("Error writing snapshot entry", e);
                    }
                });

                writer.flush();
                fos.getChannel().force(false); // fsync the temp file
            }

            // 2. Atomic rename of temp file to final snapshot file
            try {
                Files.move(
                        tempFile.toPath(),
                        targetFile.toPath(),
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING
                );
            } catch (IOException atomicMoveUnsupported) {
                // Fallback for filesystems that do not support ATOMIC_MOVE across boundaries
                Files.move(
                        tempFile.toPath(),
                        targetFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING
                );
            }

            // 3. Truncate the WAL now that snapshot is committed
            if (wal != null) {
                wal.truncate();
            }

            return targetFile;
        } finally {
            snapshotInProgress.set(false);
            if (tempFile.exists()) {
                tempFile.delete();
            }
        }
    }

    /**
     * Reads a snapshot file and populates the store.
     *
     * @param snapshotFile file to load
     * @param store        target store
     * @return number of records loaded
     */
    public static int loadSnapshot(File snapshotFile, Store store) throws IOException {
        if (!snapshotFile.exists() || snapshotFile.length() == 0) {
            return 0;
        }

        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(snapshotFile, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\\s+");
                if (parts.length >= 3) {
                    String key = parts[0];
                    String value = parts[1];
                    long expireAtMillis = Long.parseLong(parts[2]);

                    if (expireAtMillis == SimpleStore.StoredItem.NO_EXPIRY || expireAtMillis > System.currentTimeMillis()) {
                        store.setWithExactExpiry(key, value, expireAtMillis);
                        count++;
                    }
                }
            }
        }
        return count;
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
