package main.java.com.zenz.kvstore;

import com.zenz.kvstore.*;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.LogHandler;
import com.zenz.kvstore.restorers.Restorer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class KVStoreTest {
    private Path logsFolder;
    private Path snapshotsFolder;

    private KVStore store;
    private KVMapSnapshotter snapshotter;
    private WALogger logger;
    private KVStore.Builder builder;

    @BeforeEach
    void setUp() throws Exception {
        logsFolder = Files.createTempDirectory("tmp-logs-");
        snapshotsFolder = Files.createTempDirectory("tmp-snapshots-");
        snapshotter = new KVMapSnapshotter(snapshotsFolder);

        logger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler logHandler = new LogHandler(logger);

        builder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(logHandler);
        Restorer restorer = new Restorer();
        store = restorer.restore(builder);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logger != null) {
            logger.close();
        }

        logsFolder.toFile().delete();
        snapshotsFolder.toFile().delete();
    }

    private KVMap getMap(KVStore store) throws Exception {
        Field mapField = KVStore.class.getDeclaredField("map");
        mapField.setAccessible(true);
        return (KVMap) mapField.get(store);
    }

    // --- put / get ---

    @Test
    void put_thenGet_returnsNode() throws Exception {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value);

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals(value, node.value);
        assertEquals("key1", node.key);
    }

    @Test
    void get_missingKey_returnsNull() throws Exception {
        KVMap.Node node = store.get("nonexistent");
        assertNull(node);
    }

    @Test
    void put_sameKeyTwice_updatesValue() throws Exception {
        store.put("key1", "first".getBytes(StandardCharsets.UTF_8));
        store.put("key1", "second".getBytes(StandardCharsets.UTF_8));

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value);
    }

    @Test
    void put_multipleKeys_allRetrievable() throws Exception {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.put("age", ByteBuffer.allocate(4).putInt(30).array());
        store.put("city", "london".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8), store.get("name").value);
        assertEquals(30, ByteBuffer.wrap(store.get("age").value).getInt());
        assertArrayEquals("london".getBytes(StandardCharsets.UTF_8), store.get("city").value);
    }

    // --- byte[] values ---

    @Test
    void put_integerAsBytes_retrievesCorrectly() throws Exception {
        byte[] value = ByteBuffer.allocate(4).putInt(99).array();
        store.put("count", value);

        KVMap.Node node = store.get("count");

        assertNotNull(node);
        assertEquals(99, ByteBuffer.wrap(node.value).getInt());
    }

    @Test
    void put_emptyByteArray_retrievesCorrectly() throws Exception {
        store.put("empty", new byte[0]);

        KVMap.Node node = store.get("empty");

        assertNotNull(node);
        assertArrayEquals(new byte[0], node.value);
    }

    // --- large number of entries ---

    @Test
    void put_manyEntries_allRetrievable() throws Exception {
        int count = 500;
        for (int i = 0; i < count; i++) {
            store.put("key_" + i, ByteBuffer.allocate(4).putInt(i).array());
        }

        for (int i = 0; i < count; i++) {
            KVMap.Node node = store.get("key_" + i);
            assertNotNull(node, "Expected node for key_" + i);
            assertEquals(i, ByteBuffer.wrap(node.value).getInt());
        }
    }

    // --- WAL logging ---

    @Test
    void put_logsOperationToWAL() throws Exception {
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        File logFile = logsFolder.resolve("app.log").toFile();
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile.toPath());

        assertNotNull(logs, "Logs should not be null");
        assertEquals(1, logs.size(), "WAL should contain one operation");
        assertEquals(CommandType.PUT, logs.get(0).command().type(), "WAL should contain PUT operation");

        PutCommand putCmd = (PutCommand) logs.get(0).command();
        assertEquals("walKey", putCmd.key(), "WAL should contain the key");
    }

    @Test
    void get_logsOperationToWAL() throws Exception {
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        store.get("walKey");
        logger.close();

        File logFile = logsFolder.resolve("app.log").toFile();
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile.toPath());

        assertNotNull(logs, "Logs should not be null");
        assertEquals(2, logs.size(), "Should have 2 log entries");
        assertEquals(CommandType.GET, logs.get(1).command().type(), "Second entry should be GET");
    }

    @Test
    void put_andGet_bothLoggedToWAL() throws Exception {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.get("name");
        logger.close();

        File logFile = logsFolder.resolve("app.log").toFile();
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile.toPath());

        assertNotNull(logs, "Logs should not be null");
        assertEquals(2, logs.size(), "WAL should have exactly 2 entries");
        assertEquals(CommandType.PUT, logs.get(0).command().type(), "First entry should be PUT");
        assertEquals(CommandType.GET, logs.get(1).command().type(), "Second entry should be GET");
    }

    @Test
    void multipleOperations_allLoggedToWAL() throws Exception {
        store.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        store.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        store.get("k1");
        store.get("k2");
        logger.close();

        File logFile = logsFolder.resolve("app.log").toFile();
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile.toPath());

        assertNotNull(logs, "Logs should not be null");
        assertEquals(4, logs.size(), "WAL should have exactly 4 entries");

        PutCommand putCmd1 = (PutCommand) logs.get(0).command();
        assertEquals(CommandType.PUT, putCmd1.type(), "Line 1 should be PUT");
        assertEquals("k1", putCmd1.key(), "Line 1 should be PUT k1");

        PutCommand putCmd2 = (PutCommand) logs.get(1).command();
        assertEquals(CommandType.PUT, putCmd2.type(), "Line 2 should be PUT");
        assertEquals("k2", putCmd2.key(), "Line 2 should be PUT k2");

        assertEquals(CommandType.GET, logs.get(2).command().type(), "Line 3 should be GET k1");
        assertEquals(CommandType.GET, logs.get(3).command().type(), "Line 4 should be GET k2");
    }

    // --- Integration: Snapshotting ---

    @Test
    void snapshotter_createsSnapshotFromWAL() throws Exception {
        // Add some data
        store.put("snapKey1", "snapValue1".getBytes(StandardCharsets.UTF_8));
        store.put("snapKey2", "snapValue2".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Verify log file exists
        Path logFile = logsFolder.resolve("app.log");
        assertTrue(logFile.toFile().exists(), "Log file should be created");

        // Verify log contents using LogHandler
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile);
        assertNotNull(logs, "Logs should not be null");
        assertEquals(2, logs.size(), "Should have 2 log entries");

        PutCommand cmd1 = (PutCommand) logs.get(0).command();
        assertEquals("snapKey1", cmd1.key(), "First log should contain snapKey1");

        PutCommand cmd2 = (PutCommand) logs.get(1).command();
        assertEquals("snapKey2", cmd2.key(), "Second log should contain snapKey2");
    }

    @Test
    void snapshotter_restoresDataFromSnapshot() throws Exception {
        // Add some data
        store.put("restoreKey1", "restoreValue1".getBytes(StandardCharsets.UTF_8));
        store.put("restoreKey2", "restoreValue2".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshot using KVMapSnapshotter
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
//        Path fpath = Path.of(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Load snapshot - need to create new logger and logHandler for the restored store
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        System.out.println("Calling restoration within restores data from snapshot test");
        KVStore restored = new Restorer().restore(newBuilder);

        // Verify restored data
        assertNotNull(restored.get("restoreKey1"), "restoreKey1 should exist in restored store");
        assertNotNull(restored.get("restoreKey2"), "restoreKey2 should exist in restored store");
        assertArrayEquals("restoreValue1".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey1").value);
        assertArrayEquals("restoreValue2".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey2").value);

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void snapshotter_roundTrip_preservesData() throws Exception {
        // Add multiple entries
        for (int i = 0; i < 10; i++) {
            store.put("roundtripKey" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }
        logger.close();

        // Create snapshot using KVMapSnapshotter
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Load snapshot - need to create new logger and logHandler for the restored store
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify all entries
        for (int i = 0; i < 10; i++) {
            KVMap.Node node = restored.get("roundtripKey" + i);
            assertNotNull(node, "Key roundtripKey" + i + " should exist");
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
        }

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void snapshotDuringOperations_triggersWhenThresholdReached() throws Exception {
        store.setSnapshotter(snapshotter);

        // Enable snapshotting and set logs per snapshot to a small number
        store.setSnapshotEnabled(true);
        store.setLogsPerSnapshot(10);

        // Push more operations than logsPerSnapshot threshold
        int numOperations = 25;
        for (int i = 0; i < numOperations; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        logger.close();

        // Verify snapshot was created (should have triggered at 10 operations)
        File[] snapshotFiles = snapshotsFolder.toFile().listFiles();
        assertNotNull(snapshotFiles, "Snapshot folder should not be empty");
        assertTrue(snapshotFiles.length > 0, "At least one snapshot should have been created");

        // Find the snapshot file and load it
        File snapshotFile = snapshotFiles[0];
        KVMap restoredMap = snapshotter.loadSnapshot(snapshotFile.toPath());
        assertNotNull(restoredMap, "Restored map should not be null");

        // Verify some of the data exists in the restored map
        // Note: The snapshot captures state at the 10th operation, so keys 0-9 should exist
        for (int i = 0; i < 10; i++) {
            KVMap.Node node = restoredMap.get("key" + i);
            assertNotNull(node, "Key key" + i + " should exist in restored map");
        }
    }

    @Test
    void snapshotDuringLoad_triggersWhenThresholdReached() throws Exception {
        // Create store with snapshotting disabled and logsPerSnapshot = 10
        int logsPerSnapshot = 10;
        store.setSnapshotEnabled(false);
        store.setLogsPerSnapshot(logsPerSnapshot);

        // Perform 10 put operations
        for (int i = 0; i < logsPerSnapshot; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        logger.close();

        // Verify no snapshot exists before load
        File[] snapshotFilesBeforeLoad = snapshotsFolder.toFile().listFiles();
        assertEquals(0, snapshotFilesBeforeLoad.length, "Zero snapshot should exist before load");

        // Load the store using Restorer with a new LogHandler
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler)
                .setSnapshotEnabled(true)
                .setLogsPerSnapshot(logsPerSnapshot);

        KVStore restored = new Restorer().restore(newBuilder);

        // Check if during load a snapshot was created (should be 1 snapshot file)
        File[] snapshotFilesAfterLoad = snapshotsFolder.toFile().listFiles();
        assertEquals(1, snapshotFilesAfterLoad.length, "One snapshot should exist after load");

        // Verify data was restored correctly
        restored.setSnapshotEnabled(false);

        for (int i = 0; i < logsPerSnapshot; i++) {
            KVMap.Node node = restored.get("key" + i);
            assertNotNull(node, "Key key" + i + " should exist in restored store");
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
        }

        if (newLogger != null) {
            newLogger.close();
        }
    }
}
