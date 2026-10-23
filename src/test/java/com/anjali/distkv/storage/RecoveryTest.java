package com.anjali.distkv.storage;

import com.anjali.distkv.store.SimpleStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Storage Recovery & Durability Tests")
class RecoveryTest {

    @TempDir
    File tempDir;

    private File walFile;

    @BeforeEach
    void setUp() {
        walFile = new File(tempDir, "wal.log");
    }

    @AfterEach
    void tearDown() {
        // tempDir is cleaned automatically by JUnit 5
    }

    @Test
    @DisplayName("WAL append and sequential replay on fresh store")
    void testWalAppendAndReplay() throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(walFile, WriteAheadLog.FsyncPolicy.ALWAYS)) {
            wal.appendSet("user:1", "alice", SimpleStore.StoredItem.NO_EXPIRY);
            wal.appendSet("user:2", "bob", SimpleStore.StoredItem.NO_EXPIRY);
            wal.appendSet("user:3", "charlie", SimpleStore.StoredItem.NO_EXPIRY);
            wal.appendDel("user:2");
        }

        SimpleStore recoveredStore = new SimpleStore(100);
        RecoveryManager.RecoverySummary summary = RecoveryManager.recover(tempDir, recoveredStore);

        assertEquals(0, summary.snapshotRecords());
        assertEquals(4, summary.walRecordsReplayed());

        assertEquals("alice", recoveredStore.get("user:1"));
        assertNull(recoveredStore.get("user:2"));
        assertEquals("charlie", recoveredStore.get("user:3"));
    }

    @Test
    @DisplayName("Snapshot creation truncates WAL and allows full recovery")
    void testSnapshotAndWalTruncation() throws IOException {
        SimpleStore liveStore = new SimpleStore(100);
        liveStore.set("k1", "v1", OptionalLong.empty());
        liveStore.set("k2", "v2", OptionalLong.empty());

        WriteAheadLog wal = new WriteAheadLog(walFile, WriteAheadLog.FsyncPolicy.ALWAYS);
        wal.appendSet("k1", "v1", SimpleStore.StoredItem.NO_EXPIRY);
        wal.appendSet("k2", "v2", SimpleStore.StoredItem.NO_EXPIRY);

        assertTrue(walFile.length() > 0);

        // Take snapshot
        SnapshotManager snapshotManager = new SnapshotManager(tempDir);
        File snapshot = snapshotManager.takeSnapshot(liveStore, wal);

        assertTrue(snapshot.exists());
        assertTrue(snapshot.length() > 0);
        assertEquals(0, walFile.length(), "WAL must be truncated after snapshot");

        // Mutate store with further WAL updates after snapshot
        wal.appendSet("k3", "v3", SimpleStore.StoredItem.NO_EXPIRY);
        wal.appendDel("k1");
        wal.close();

        // Recover into a clean store
        SimpleStore restoredStore = new SimpleStore(100);
        RecoveryManager.RecoverySummary summary = RecoveryManager.recover(tempDir, restoredStore);

        assertEquals(2, summary.snapshotRecords());
        assertEquals(2, summary.walRecordsReplayed());

        assertNull(restoredStore.get("k1"), "k1 was deleted after snapshot via WAL");
        assertEquals("v2", restoredStore.get("k2"), "k2 came from snapshot");
        assertEquals("v3", restoredStore.get("k3"), "k3 came from post-snapshot WAL");
    }

    @Test
    @DisplayName("Recovery safely handles partially written / corrupted trailing log record")
    void testToleratePartiallyWrittenLastRecord() throws IOException {
        // 1. Write valid records
        try (WriteAheadLog wal = new WriteAheadLog(walFile, WriteAheadLog.FsyncPolicy.ALWAYS)) {
            wal.appendSet("valid1", "data1", SimpleStore.StoredItem.NO_EXPIRY);
            wal.appendSet("valid2", "data2", SimpleStore.StoredItem.NO_EXPIRY);
        }

        // 2. Simulate sudden power failure / kill -9 writing half a record at EOF
        try (FileOutputStream fos = new FileOutputStream(walFile, true)) {
            // Half-written command without arguments or newline
            byte[] partialBytes = "SET corrupted_key incomp".getBytes(StandardCharsets.UTF_8);
            fos.write(partialBytes);
            fos.flush();
        }

        // 3. Replay must successfully recover valid1 and valid2 without throwing exception
        SimpleStore store = new SimpleStore(100);
        RecoveryManager.RecoverySummary summary = RecoveryManager.recover(tempDir, store);

        assertEquals(2, summary.walRecordsReplayed());
        assertEquals("data1", store.get("valid1"));
        assertEquals("data2", store.get("valid2"));
        assertNull(store.get("corrupted_key"));
    }

    @Test
    @DisplayName("FsyncPolicy enum parsing works correctly")
    void testFsyncPolicyParsing() {
        assertEquals(WriteAheadLog.FsyncPolicy.ALWAYS, WriteAheadLog.FsyncPolicy.fromString("always"));
        assertEquals(WriteAheadLog.FsyncPolicy.ALWAYS, WriteAheadLog.FsyncPolicy.fromString("everywrite"));
        assertEquals(WriteAheadLog.FsyncPolicy.NEVER, WriteAheadLog.FsyncPolicy.fromString("never"));
        assertEquals(WriteAheadLog.FsyncPolicy.EVERY_SECOND, WriteAheadLog.FsyncPolicy.fromString("everysec"));
        assertEquals(WriteAheadLog.FsyncPolicy.EVERY_SECOND, WriteAheadLog.FsyncPolicy.fromString("unknown"));
    }
}
