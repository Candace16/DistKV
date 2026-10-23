package com.anjali.distkv.storage;

import com.anjali.distkv.store.SimpleStore;
import com.anjali.distkv.store.Store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Orchestrates startup recovery from persistent storage.
 *
 * <p>Recovery Sequence:
 * <ol>
 *   <li>Loads the latest committed snapshot ({@code snapshot.db}), if present.</li>
 *   <li>Replays subsequent delta mutations from the Write-Ahead Log ({@code wal.log}).</li>
 *   <li><b>Fault-tolerant partial write handling:</b> If the process crashed in the middle
 *       of writing the final log record, any corrupted or truncated trailing line is
 *       safely ignored without aborting recovery.</li>
 * </ol>
 */
public final class RecoveryManager {

    public record RecoverySummary(int snapshotRecords, int walRecordsReplayed) {}

    private RecoveryManager() {}

    /**
     * Reconstructs the in-memory state of the given store from disk.
     *
     * @param dataDir directory holding snapshot.db and wal.log
     * @param store   store instance to populate
     * @return summary of restored items
     */
    public static RecoverySummary recover(File dataDir, Store store) throws IOException {
        int snapshotCount = 0;
        int walCount = 0;

        // 1. Recover from snapshot
        File snapshotFile = new File(dataDir, SnapshotManager.SNAPSHOT_FILE_NAME);
        if (snapshotFile.exists() && snapshotFile.length() > 0) {
            snapshotCount = SnapshotManager.loadSnapshot(snapshotFile, store);
        }

        // 2. Replay Write-Ahead Log
        File walFile = new File(dataDir, "wal.log");
        if (walFile.exists() && walFile.length() > 0) {
            walCount = replayWal(walFile, store);
        }

        return new RecoverySummary(snapshotCount, walCount);
    }

    /**
     * Replays records from wal.log in strict sequential order.
     * Tolerates partially written or truncated trailing lines caused by abrupt crashes.
     */
    public static int replayWal(File walFile, Store store) throws IOException {
        int replayedCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(walFile, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\\s+");
                if (parts.length == 0) {
                    continue;
                }

                String command = parts[0].toUpperCase();
                try {
                    if ("SET".equals(command)) {
                        if (parts.length < 4) {
                            // Incomplete/partial trailing record at EOF; abort replay cleanly
                            System.err.println("[Recovery] Skipped partial/corrupted trailing SET record: " + line);
                            break;
                        }
                        String key = parts[1];
                        String value = parts[2];
                        long expireAt = Long.parseLong(parts[3]);

                        if (expireAt == SimpleStore.StoredItem.NO_EXPIRY || expireAt > System.currentTimeMillis()) {
                            store.setWithExactExpiry(key, value, expireAt);
                            replayedCount++;
                        }
                    } else if ("DEL".equals(command)) {
                        if (parts.length < 2) {
                            System.err.println("[Recovery] Skipped partial/corrupted trailing DEL record: " + line);
                            break;
                        }
                        store.del(parts[1]);
                        replayedCount++;
                    } else {
                        // Unrecognized format; likely corrupted tail
                        System.err.println("[Recovery] Warning: unrecognized/corrupted log record: " + line);
                        break;
                    }
                } catch (NumberFormatException e) {
                    // Truncated integer at tail of file
                    System.err.println("[Recovery] Parse failure in trailing log line (partial write): " + line);
                    break;
                }
            }
        }

        return replayedCount;
    }
}
