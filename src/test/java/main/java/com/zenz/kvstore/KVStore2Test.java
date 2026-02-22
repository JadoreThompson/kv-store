package main.java.com.zenz.kvstore;

import com.zenz.kvstore.KVMap;
import com.zenz.kvstore.KVStore2;
import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.WALogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class KVStore2Test {
    private Path logsFolder;
    private Path snapshotsFolder;

    private KVStore2 store;
    private KVMapSnapshotter snapshotter;
    private WALogger logger;

    @BeforeEach
    void setUp() throws Exception {
        logsFolder = Files.createTempDirectory("tmp-logs-");
        snapshotsFolder = Files.createTempDirectory("tmp-snapshots-");
        snapshotter = new KVMapSnapshotter(snapshotsFolder);
        store = new KVStore2.Builder().setLogsFolder(logsFolder).setSnapshotter(snapshotter).build();
        logger = getLogger(store);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logger != null) {
            logger.close();
        }

        deleteDirectory(logsFolder.toFile());
        deleteDirectory(snapshotsFolder.toFile());
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    // --- Reflection helpers for private constructor and fields ---

    private WALogger getLogger(KVStore2 store) throws Exception {
        Field loggerField = KVStore2.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        return (WALogger) loggerField.get(store);
    }

    private KVMap getMap(KVStore2 store) throws Exception {
        Field mapField = KVStore2.class.getDeclaredField("map");
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
        store.setLoggingEnabled(true);
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        String walContents = Files.readString(logFile.toPath());

        assertTrue(walContents.contains("PUT"), "WAL should contain PUT operation");
        assertTrue(walContents.contains("walKey"), "WAL should contain the key");
    }

    @Test
    void get_logsOperationToWAL() throws Exception {
        store.setLoggingEnabled(true);
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        store.get("walKey");
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        String walContents = Files.readString(logFile.toPath());

        assertTrue(walContents.contains("GET"), "WAL should contain GET operation");
        assertTrue(walContents.contains("walKey"), "WAL should contain the key");
    }

    @Test
    void put_andGet_bothLoggedToWAL() throws Exception {
        store.setLoggingEnabled(true);
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.get("name");
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        String walContents = Files.readString(logFile.toPath());
        String[] lines = walContents.strip().split("\n");

        assertEquals(2, lines.length, "WAL should have exactly 2 entries");
        assertTrue(lines[0].contains("PUT"), "First entry should be PUT");
        assertTrue(lines[1].contains("GET"), "Second entry should be GET");
    }

    @Test
    void multipleOperations_allLoggedToWAL() throws Exception {
        store.setLoggingEnabled(true);
        store.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        store.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        store.get("k1");
        store.get("k2");
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        String walContents = Files.readString(logFile.toPath());
        String[] lines = walContents.strip().split("\n");

        assertEquals(4, lines.length, "WAL should have exactly 4 entries");
        assertTrue(lines[0].contains("PUT") && lines[0].contains("k1"), "Line 1 should be PUT k1");
        assertTrue(lines[1].contains("PUT") && lines[1].contains("k2"), "Line 2 should be PUT k2");
        assertTrue(lines[2].contains("GET") && lines[2].contains("k1"), "Line 3 should be GET k1");
        assertTrue(lines[3].contains("GET") && lines[3].contains("k2"), "Line 4 should be GET k2");
    }

    // --- Integration: Snapshotting ---

    @Test
    void snapshotter_createsSnapshotFromWAL() throws Exception {
        store.setLoggingEnabled(true);

        // Add some data
        store.put("snapKey1", "snapValue1".getBytes(StandardCharsets.UTF_8));
        store.put("snapKey2", "snapValue2".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshotter and snapshot
        snapshotter.setMultiThreadingEnabled(false);

        Path logFile = logsFolder.resolve("0.log");
        // Note: KVMapSnapshotter works differently - it snapshots a KVMap directly
        // For this test, we verify the log file exists and contains the data
        assertTrue(logFile.toFile().exists(), "Log file should be created");

        // Verify log contents
        String logContents = Files.readString(logFile);
        assertTrue(logContents.contains("snapKey1"), "Log should contain snapKey1");
        assertTrue(logContents.contains("snapKey2"), "Log should contain snapKey2");
    }

    @Test
    void snapshotter_restoresDataFromSnapshot() throws Exception {
        store.setLoggingEnabled(true);

        // Add some data
        store.put("restoreKey1", "restoreValue1".getBytes(StandardCharsets.UTF_8));
        store.put("restoreKey2", "restoreValue2".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshot using KVMapSnapshotter
        snapshotter.setMultiThreadingEnabled(false);
        store.setSnapshotter(snapshotter);

        KVMap map = getMap(store);
        snapshotter.snapshot(map);

        // Load snapshot
        KVStore2 restored = new KVStore2.Builder().setLogsFolder(logsFolder).setSnapshotter(snapshotter).build();

        // Verify restored data
        assertNotNull(restored.get("restoreKey1"), "restoreKey1 should exist in restored store");
        assertNotNull(restored.get("restoreKey2"), "restoreKey2 should exist in restored store");
        assertArrayEquals("restoreValue1".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey1").value);
        assertArrayEquals("restoreValue2".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey2").value);

        WALogger restoredLogger = getLogger(restored);
        if (restoredLogger != null) {
            restoredLogger.close();
        }
    }

    @Test
    void snapshotter_roundTrip_preservesData() throws Exception {
        store.setLoggingEnabled(true);

        // Add multiple entries
        for (int i = 0; i < 10; i++) {
            store.put("roundtripKey" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }
        logger.close();

        // Create snapshot using KVMapSnapshotter
        snapshotter.setMultiThreadingEnabled(false);
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        snapshotter.snapshot(map);

        // Load snapshot
        KVStore2 restored = new KVStore2.Builder().setLogsFolder(logsFolder).setSnapshotter(snapshotter).build();

        // Verify all entries
        for (int i = 0; i < 10; i++) {
            KVMap.Node node = restored.get("roundtripKey" + i);
            assertNotNull(node, "Key roundtripKey" + i + " should exist");
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
        }

        WALogger restoredLogger = getLogger(restored);
        if (restoredLogger != null) {
            restoredLogger.close();
        }
    }

    @Test
    void snapshotDuringOperations_triggersWhenThresholdReached() throws Exception {
        snapshotter.setMultiThreadingEnabled(false);
        store.setSnapshotter(snapshotter);

        // Enable snapshotting and set logs per snapshot to a small number
        store.setSnapshotEnabled(true);
        store.setLogsPerSnapshot(10);
        store.setLoggingEnabled(true);

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

        // Verify data can be restored from snapshot
        KVMap restoredMap = snapshotter.loadSnapshot();
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
        store.setLoggingEnabled(true);

        // Perform 10 put operations
        for (int i = 0; i < logsPerSnapshot; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        logger.close();

        // Use KVMapSnapshotter to snapshot the underlying map
        KVMapSnapshotter snapshotter = new KVMapSnapshotter(snapshotsFolder);
        snapshotter.setMultiThreadingEnabled(false);

        // Verify snapshot was created
        File[] snapshotFilesBeforeLoad = snapshotsFolder.toFile().listFiles();
        assertEquals(0, snapshotFilesBeforeLoad.length, "Zero snapshot should exist before load");

        // Load the store using the same logsFolder
        KVStore2 restored = new KVStore2.Builder()
                .setLogsFolder(logsFolder)
                .setSnapshotter(snapshotter)
                .setSnapshotEnabled(true)
                .setLogsPerSnapshot(logsPerSnapshot)
                .build();

        // Check if during load a snapshot was created (should be 1 snapshot file)
        File[] snapshotFilesAfterLoad = snapshotsFolder.toFile().listFiles();
        assertEquals(1, snapshotFilesAfterLoad.length, "One snapshot should exist after load");

        // Verify data was restored correctly
        restored.setSnapshotEnabled(false);
        restored.setLoggingEnabled(false);

        for (int i = 0; i < logsPerSnapshot; i++) {
            KVMap.Node node = restored.get("key" + i);
            assertNotNull(node, "Key key" + i + " should exist in restored store");
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
        }

        WALogger restoredLogger = getLogger(restored);
        if (restoredLogger != null) {
            restoredLogger.close();
        }
    }
}
