package com.zenz.kvstore.server;

import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handler.LogHandler;
import com.zenz.kvstore.server.restorer.Restorer;
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
import java.util.List;

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

    @Test
    void put_thenGet_returnsNode() throws Exception {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value);

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals(value, node.value());
        assertEquals("key1", node.key());
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
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value());
    }

    @Test
    void put_multipleKeys_allRetrievable() throws Exception {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.put("age", ByteBuffer.allocate(4).putInt(30).array());
        store.put("city", "london".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8), store.get("name").value());
        assertEquals(30, ByteBuffer.wrap(store.get("age").value()).getInt());
        assertArrayEquals("london".getBytes(StandardCharsets.UTF_8), store.get("city").value());
    }

    @Test
    void put_integerAsBytes_retrievesCorrectly() throws Exception {
        byte[] value = ByteBuffer.allocate(4).putInt(99).array();
        store.put("count", value);

        KVMap.Node node = store.get("count");

        assertNotNull(node);
        assertEquals(99, ByteBuffer.wrap(node.value()).getInt());
    }

    @Test
    void put_emptyByteArray_retrievesCorrectly() throws Exception {
        store.put("empty", new byte[0]);

        KVMap.Node node = store.get("empty");

        assertNotNull(node);
        assertArrayEquals(new byte[0], node.value());
    }

    @Test
    void put_manyEntries_allRetrievable() throws Exception {
        int count = 500;
        for (int i = 0; i < count; i++) {
            store.put("key_" + i, ByteBuffer.allocate(4).putInt(i).array());
        }

        for (int i = 0; i < count; i++) {
            KVMap.Node node = store.get("key_" + i);
            assertNotNull(node, "Expected node for key_" + i);
            assertEquals(i, ByteBuffer.wrap(node.value()).getInt());
        }
    }

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
        assertEquals(1, logs.size(), "Should have 2 log entries");
        assertEquals(CommandType.PUT, logs.get(0).command().type(), "First entry should be PUT");
    }

    @Test
    void put_andGet_bothLoggedToWAL() throws Exception {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.get("name");
        logger.close();

        File logFile = logsFolder.resolve("app.log").toFile();
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile.toPath());

        assertNotNull(logs, "Logs should not be null");
        assertEquals(1, logs.size(), "WAL should have exactly 2 entries");
        assertEquals(CommandType.PUT, logs.get(0).command().type(), "First entry should be PUT");
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
        assertEquals(2, logs.size(), "WAL should have exactly 2 entries");

        PutCommand putCmd1 = (PutCommand) logs.get(0).command();
        assertEquals(CommandType.PUT, putCmd1.type(), "Line 1 should be PUT");
        assertEquals("k1", putCmd1.key(), "Line 1 should be PUT k1");

        PutCommand putCmd2 = (PutCommand) logs.get(1).command();
        assertEquals(CommandType.PUT, putCmd2.type(), "Line 2 should be PUT");
        assertEquals("k2", putCmd2.key(), "Line 2 should be PUT k2");
    }

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
        snapshotter.snapshot(map, fpath);

        // Load snapshot - need to create new logger and logHandler for the restored store
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify restored data
        assertNotNull(restored.get("restoreKey1"), "restoreKey1 should exist in restored store");
        assertNotNull(restored.get("restoreKey2"), "restoreKey2 should exist in restored store");
        assertArrayEquals("restoreValue1".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey1").value());
        assertArrayEquals("restoreValue2".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey2").value());

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
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value());
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
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value());
        }

        if (newLogger != null) {
            newLogger.close();
        }
    }

    // ==================== DELETE UNIT TESTS ====================

    @Test
    void delete_existingKey_returnsTrue() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete("key1");

        assertTrue(result, "Delete should return true for existing key");
    }

    @Test
    void delete_missingKey_returnsFalse() throws Exception {
        boolean result = store.delete("nonexistent");

        assertFalse(result, "Delete should return false for missing key");
    }

    @Test
    void delete_existingKey_keyNoLongerRetrievable() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        store.delete("key1");

        KVMap.Node node = store.get("key1");
        assertNull(node, "Key should no longer exist after delete");
    }

    @Test
    void delete_sameKeyTwice_secondReturnsFalse() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        boolean firstDelete = store.delete("key1");
        boolean secondDelete = store.delete("key1");

        assertTrue(firstDelete, "First delete should return true");
        assertFalse(secondDelete, "Second delete should return false");
    }

    @Test
    void delete_emptyKey_handlesCorrectly() throws Exception {
        store.put("", "emptyKeyValue".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete("");

        assertTrue(result, "Delete should handle empty key");
        assertNull(store.get(""), "Empty key should be removed");
    }

    @Test
    void delete_oneKey_otherKeysRemain() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        store.delete("key2");

        assertNotNull(store.get("key1"), "key1 should still exist");
        assertNull(store.get("key2"), "key2 should be deleted");
        assertNotNull(store.get("key3"), "key3 should still exist");
    }

    @Test
    void delete_logsOperationToWAL() throws Exception {
        store.put("deleteKey", "deleteValue".getBytes(StandardCharsets.UTF_8));
        store.delete("deleteKey");
        logger.close();

        File logFile = logsFolder.resolve("app.log").toFile();
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile.toPath());

        assertNotNull(logs, "Logs should not be null");
        assertEquals(2, logs.size(), "WAL should contain two operations");
        assertEquals(CommandType.PUT, logs.get(0).command().type(), "First entry should be PUT");
        assertEquals(CommandType.DELETE, logs.get(1).command().type(), "Second entry should be DELETE");

        DeleteCommand deleteCmd = (DeleteCommand) logs.get(1).command();
        assertEquals("deleteKey", deleteCmd.key(), "WAL should contain the deleted key");
    }

    @Test
    void delete_missingKey_logsOperationToWAL() throws Exception {
        store.delete("nonexistent");
        logger.close();

        File logFile = logsFolder.resolve("app.log").toFile();
        ArrayList<LogHandler.Log> logs = LogHandler.deserialize(logFile.toPath());

        assertNotNull(logs, "Logs should not be null");
        assertEquals(1, logs.size(), "WAL should contain one operation");
        assertEquals(CommandType.DELETE, logs.get(0).command().type(), "Entry should be DELETE");

        DeleteCommand deleteCmd = (DeleteCommand) logs.get(0).command();
        assertEquals("nonexistent", deleteCmd.key(), "WAL should contain the key");
    }

    // ==================== DELETE INTEGRATION TESTS ====================

    @Test
    void delete_afterPut_removesValue() throws Exception {
        store.put("toDelete", "value".getBytes(StandardCharsets.UTF_8));

        boolean deleted = store.delete("toDelete");

        assertTrue(deleted, "Delete should succeed");
        assertNull(store.get("toDelete"), "Value should be removed after delete");
    }

    @Test
    void delete_multipleKeys_allRemoved() throws Exception {
        int count = 10;
        for (int i = 0; i < count; i++) {
            store.put("key_" + i, ("value_" + i).getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < count; i++) {
            boolean result = store.delete("key_" + i);
            assertTrue(result, "Delete should succeed for key_" + i);
        }

        for (int i = 0; i < count; i++) {
            assertNull(store.get("key_" + i), "Key key_" + i + " should be deleted");
        }
    }

    @Test
    void delete_interleavedWithPut_handlesCorrectly() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.delete("key1");
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        store.delete("key2");
        store.put("key1", "newValue1".getBytes(StandardCharsets.UTF_8));

        assertNotNull(store.get("key1"), "key1 should exist");
        assertNull(store.get("key2"), "key2 should be deleted");
        assertNotNull(store.get("key3"), "key3 should exist");
        assertArrayEquals("newValue1".getBytes(StandardCharsets.UTF_8), store.get("key1").value());
    }

    @Test
    void delete_thenPutSameKey_keyRetrievable() throws Exception {
        store.put("key1", "originalValue".getBytes(StandardCharsets.UTF_8));
        store.delete("key1");
        store.put("key1", "newValue".getBytes(StandardCharsets.UTF_8));

        KVMap.Node node = store.get("key1");

        assertNotNull(node, "Key should exist after re-put");
        assertArrayEquals("newValue".getBytes(StandardCharsets.UTF_8), node.value());
    }

    @Test
    void delete_duringSnapshot_triggersWhenThresholdReached() throws Exception {
        store.setSnapshotter(snapshotter);
        store.setSnapshotEnabled(true);
        store.setLogsPerSnapshot(10);

        // Add 5 entries
        for (int i = 0; i < 5; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Delete 5 entries (total 10 operations = threshold)
        for (int i = 0; i < 5; i++) {
            store.delete("key" + i);
        }

        logger.close();

        // Verify snapshot was created
        File[] snapshotFiles = snapshotsFolder.toFile().listFiles();
        assertNotNull(snapshotFiles, "Snapshot folder should not be empty");
        assertTrue(snapshotFiles.length > 0, "At least one snapshot should have been created");
    }

    @Test
    void delete_restoredFromSnapshot_keyNotFound() throws Exception {
        store.put("toDelete", "value".getBytes(StandardCharsets.UTF_8));
        store.put("toKeep", "keepValue".getBytes(StandardCharsets.UTF_8));
        store.delete("toDelete");
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore from snapshot
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        assertNull(restored.get("toDelete"), "Deleted key should not exist in restored store");
        assertNotNull(restored.get("toKeep"), "Other key should exist in restored store");

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void delete_snapshotRoundTrip_preservesDeleteState() throws Exception {
        // Add entries and delete some
        for (int i = 0; i < 10; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }
        // Delete even keys
        for (int i = 0; i < 10; i += 2) {
            store.delete("key" + i);
        }
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore from snapshot
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify even keys are deleted, odd keys exist
        for (int i = 0; i < 10; i += 2) {
            assertNull(restored.get("key" + i), "Even key" + i + " should be deleted");
        }
        for (int i = 1; i < 10; i += 2) {
            assertNotNull(restored.get("key" + i), "Odd key" + i + " should exist");
        }

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void delete_largeValue_handlesCorrectly() throws Exception {
        byte[] largeValue = new byte[10000];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        store.put("largeKey", largeValue);

        boolean result = store.delete("largeKey");

        assertTrue(result, "Delete should handle large values");
        assertNull(store.get("largeKey"), "Large value key should be deleted");
    }

    @Test
    void delete_specialCharacterKey_handlesCorrectly() throws Exception {
        String specialKey = "key-with-special_chars!@#$%^&*()";
        store.put(specialKey, "value".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete(specialKey);

        assertTrue(result, "Delete should handle special character keys");
        assertNull(store.get(specialKey), "Special character key should be deleted");
    }

    @Test
    void delete_unicodeKey_handlesCorrectly() throws Exception {
        String unicodeKey = "键值"; // Chinese characters
        store.put(unicodeKey, "unicodeValue".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete(unicodeKey);

        assertTrue(result, "Delete should handle unicode keys");
        assertNull(store.get(unicodeKey), "Unicode key should be deleted");
    }

    // ==================== SEARCH OPERATION TESTS ====================

    @Test
    void search_withMatchingPrefix_returnsMatchingNodes() throws Exception {
        store.put("apple", "fruit".getBytes(StandardCharsets.UTF_8));
        store.put("application", "software".getBytes(StandardCharsets.UTF_8));
        store.put("apply", "action".getBytes(StandardCharsets.UTF_8));
        store.put("banana", "fruit".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("app");

        assertEquals(3, result.size(), "Should find 3 nodes with 'app' prefix");
        assertTrue(result.stream().anyMatch(n -> n.key().equals("apple")));
        assertTrue(result.stream().anyMatch(n -> n.key().equals("application")));
        assertTrue(result.stream().anyMatch(n -> n.key().equals("apply")));
        assertFalse(result.stream().anyMatch(n -> n.key().equals("banana")));
    }

    @Test
    void search_withNoMatch_returnsEmptyList() throws Exception {
        store.put("hello", "world".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("xyz");

        assertNotNull(result, "Search should return non-null list");
        assertTrue(result.isEmpty(), "Search should return empty list for no matches");
    }

    @Test
    void search_emptyPrefix_returnsAllNodes() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("");

        assertEquals(3, result.size(), "Empty prefix should return all nodes");
    }

    @Test
    void search_exactKeyMatch_returnsNode() throws Exception {
        store.put("exactKey", "exactValue".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("exactKey");

        assertEquals(1, result.size(), "Should find exactly one node");
        assertEquals("exactKey", result.get(0).key());
        assertArrayEquals("exactValue".getBytes(StandardCharsets.UTF_8), result.get(0).value());
    }

    @Test
    void search_emptyStore_returnsEmptyList() throws Exception {
        List<KVMap.Node> result = store.search("anything");

        assertNotNull(result, "Search should return non-null list");
        assertTrue(result.isEmpty(), "Search on empty store should return empty list");
    }

    @Test
    void search_afterDelete_excludesDeletedKey() throws Exception {
        store.put("apple", "fruit".getBytes(StandardCharsets.UTF_8));
        store.put("application", "software".getBytes(StandardCharsets.UTF_8));
        store.delete("apple");

        List<KVMap.Node> result = store.search("app");

        assertEquals(1, result.size(), "Should find 1 node after delete");
        assertEquals("application", result.get(0).key());
    }

    @Test
    void search_afterUpdate_returnsUpdatedValue() throws Exception {
        store.put("key1", "oldValue".getBytes(StandardCharsets.UTF_8));
        store.put("key1", "newValue".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("key");

        assertEquals(1, result.size());
        assertArrayEquals("newValue".getBytes(StandardCharsets.UTF_8), result.get(0).value());
    }

    @Test
    void search_singleCharacterPrefix_returnsMatches() throws Exception {
        store.put("a", "letterA".getBytes(StandardCharsets.UTF_8));
        store.put("ab", "lettersAB".getBytes(StandardCharsets.UTF_8));
        store.put("abc", "lettersABC".getBytes(StandardCharsets.UTF_8));
        store.put("b", "letterB".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("a");

        assertEquals(3, result.size(), "Should find 3 nodes starting with 'a'");
        assertTrue(result.stream().allMatch(n -> n.key().startsWith("a")));
    }

    @Test
    void search_caseSensitive_returnsCorrectMatches() throws Exception {
        store.put("Apple", "fruit".getBytes(StandardCharsets.UTF_8));
        store.put("apple", "tech".getBytes(StandardCharsets.UTF_8));
        store.put("APPLE", "uppercase".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> lowerResult = store.search("a");
        List<KVMap.Node> upperResult = store.search("A");
        List<KVMap.Node> allCapsResult = store.search("AP");

        assertEquals(1, lowerResult.size(), "Should find 1 lowercase 'apple'");
        assertEquals("apple", lowerResult.get(0).key());

        assertEquals(2, upperResult.size(), "Should find 2 title case 'Apple' and 'APPLE'");
        assertEquals("Apple", upperResult.get(0).key());

        assertEquals(1, allCapsResult.size(), "Should find 1 uppercase 'APPLE'");
        assertEquals("APPLE", allCapsResult.get(0).key());
    }

    @Test
    void search_specialCharacterPrefix_returnsMatches() throws Exception {
        store.put("key-1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key-2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key_1", "value3".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("key-");

        assertEquals(2, result.size(), "Should find 2 nodes with 'key-' prefix");
        assertTrue(result.stream().allMatch(n -> n.key().startsWith("key-")));
    }

    @Test
    void search_numericPrefix_returnsMatches() throws Exception {
        store.put("123", "numeric".getBytes(StandardCharsets.UTF_8));
        store.put("123abc", "alphanumeric".getBytes(StandardCharsets.UTF_8));
        store.put("456", "other".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("123");

        assertEquals(2, result.size(), "Should find 2 nodes with '123' prefix");
    }

    @Test
    void search_unicodePrefix_returnsMatches() throws Exception {
        store.put("日本語", "japanese".getBytes(StandardCharsets.UTF_8));
        store.put("日本料理", "cuisine".getBytes(StandardCharsets.UTF_8));
        store.put("日历", "calendar".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("日本");

        assertEquals(2, result.size(), "Should find 2 nodes with '日本' prefix");
    }

    @Test
    void search_multipleOperations_maintainsConsistency() throws Exception {
        // Add initial keys
        store.put("test1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("test2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("other", "value3".getBytes(StandardCharsets.UTF_8));

        // Search and verify
        List<KVMap.Node> result1 = store.search("test");
        assertEquals(2, result1.size());

        // Delete one key
        store.delete("test1");

        // Search again
        List<KVMap.Node> result2 = store.search("test");
        assertEquals(1, result2.size());
        assertEquals("test2", result2.get(0).key());

        // Add new key
        store.put("test3", "value3".getBytes(StandardCharsets.UTF_8));

        // Search again
        List<KVMap.Node> result3 = store.search("test");
        assertEquals(2, result3.size());
    }

    // ==================== TRIE STATE REPLENISHMENT FROM SNAPSHOTS ====================

    @Test
    void search_afterRestoreFromSnapshot_returnsCorrectResults() throws Exception {
        // Add data
        store.put("prefix1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("prefix2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("other", "value3".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore from snapshot
        WALogger newLogger = new WALogger(logsFolder.resolve("new.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify search works on restored store
        List<KVMap.Node> result = restored.search("prefix");
        assertEquals(2, result.size(), "Search should find 2 nodes with 'prefix' after restore");
        assertTrue(result.stream().anyMatch(n -> n.key().equals("prefix1")));
        assertTrue(result.stream().anyMatch(n -> n.key().equals("prefix2")));

        List<KVMap.Node> otherResult = restored.search("other");
        assertEquals(1, otherResult.size());

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterRestoreFromWAL_returnsCorrectResults() throws Exception {
        // Add data (this gets logged to WAL)
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Restore from WAL (no snapshot exists)

        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(this.builder.getLogHandler());

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify search works after WAL restore
        List<KVMap.Node> result = restored.search("key");
        assertEquals(3, result.size(), "Search should find all 3 keys after WAL restore");


    }

    @Test
    void search_afterRestoreFromSnapshotAndWAL_returnsCorrectResults() throws Exception {
        // Add initial data and create snapshot
        store.put("snap1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("snap2", "value2".getBytes(StandardCharsets.UTF_8));
        logger.close();

        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Add more data to WAL (after snapshot)
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
//        store = new KVStore.Builder()
//                .setSnapshotter(snapshotter)
//                .setLogHandler(newLogHandler)
//                .setMap(map);

        store = new Restorer().restore(new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler)
                .setMap(map));

        store.put("wal1", "value3".getBytes(StandardCharsets.UTF_8));
        store.put("snap1", "updatedValue1".getBytes(StandardCharsets.UTF_8)); // Update existing key
        newLogger.close();

        // Restore from both snapshot and WAL
        WALogger restoreLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler restoreLogHandler = new LogHandler(restoreLogger);
        KVStore.Builder restoreBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(restoreLogHandler);

        KVStore restored = new Restorer().restore(restoreBuilder);

        // Verify search includes both snapshot and WAL data
        List<KVMap.Node> snapResult = restored.search("snap");
        assertEquals(2, snapResult.size(), "Should find snapshot keys");

        List<KVMap.Node> walResult = restored.search("wal");
        assertEquals(1, walResult.size(), "Should find WAL keys");

        // Verify updated value
        List<KVMap.Node> updatedResult = restored.search("snap1");
        assertEquals(1, updatedResult.size());
        assertArrayEquals("updatedValue1".getBytes(StandardCharsets.UTF_8), updatedResult.get(0).value());

        if (restoreLogger != null) {
            restoreLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithDeletes_returnsCorrectResults() throws Exception {
        // Add data
        store.put("keep1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("delete1", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("keep2", "value3".getBytes(StandardCharsets.UTF_8));
        store.delete("delete1");
        logger.close();

        // Create snapshot after delete
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify deleted key is not in search results
        List<KVMap.Node> result = restored.search("keep");
        assertEquals(2, result.size());

        List<KVMap.Node> deletedResult = restored.search("delete");
        assertTrue(deletedResult.isEmpty(), "Deleted key should not appear in search");

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterRestoreFromWALWithDeletes_returnsCorrectResults() throws Exception {
        // Add data and delete via WAL
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        store.delete("key2");
        logger.close();

        // Restore from WAL (no snapshot)
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify search results
        List<KVMap.Node> result = restored.search("key");
        assertEquals(2, result.size(), "Should find 2 keys (key2 was deleted)");
        assertTrue(result.stream().anyMatch(n -> n.key().equals("key1")));
        assertTrue(result.stream().anyMatch(n -> n.key().equals("key3")));
        assertFalse(result.stream().anyMatch(n -> n.key().equals("key2")));

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_largeDatasetAfterRestore_returnsCorrectResults() throws Exception {
        // Add large dataset
        int count = 100;
        for (int i = 0; i < count; i++) {
            store.put("key" + String.format("%03d", i), ("value" + i).getBytes(StandardCharsets.UTF_8));
        }
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify all keys are searchable
        List<KVMap.Node> result = restored.search("key");
        assertEquals(count, result.size(), "All keys should be searchable after restore");

        // Verify specific prefix searches
        List<KVMap.Node> prefix05 = restored.search("key05");
        assertEquals(10, prefix05.size(), "Should find 10 keys with prefix 'key05'");

        if (newLogger != null) {
            newLogger.close();
        }
    }

    // ==================== EDGE CASES FOR SNAPSHOT RELOADING ====================

    @Test
    void search_emptySnapshot_returnsEmptyList() throws Exception {
        // Create empty snapshot
        logger.close();
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore from empty snapshot
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify search returns empty
        List<KVMap.Node> result = restored.search("anything");
        assertTrue(result.isEmpty(), "Search on empty restored store should return empty list");

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithOverlappingKeys_returnsCorrectResults() throws Exception {
        // Create snapshot with initial keys
        store.put("key1", "snapshotValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Add WAL entries that overlap with snapshot
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
//        store = new KVStore.Builder()
//                .setSnapshotter(snapshotter)
//                .setLogHandler(newLogHandler)
//                .setMap(map)
//                .build();

        store = new Restorer().restore(new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler)
                .setMap(map));

        store.put("key1", "walUpdatedValue".getBytes(StandardCharsets.UTF_8)); // Update existing
        store.put("key2", "newKey".getBytes(StandardCharsets.UTF_8)); // New key
        newLogger.close();

        // Restore
        WALogger restoreLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler restoreLogHandler = new LogHandler(restoreLogger);
        KVStore.Builder restoreBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(restoreLogHandler);

        KVStore restored = new Restorer().restore(restoreBuilder);

        // Verify the WAL update took precedence
        List<KVMap.Node> result = restored.search("key");
        assertEquals(2, result.size());

        KVMap.Node key1Node = restored.get("key1");
        assertArrayEquals("walUpdatedValue".getBytes(StandardCharsets.UTF_8), key1Node.value(),
                "WAL update should override snapshot value");

        if (restoreLogger != null) {
            restoreLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithKeyDeletedInWAL_keyNotFound() throws Exception {
        // Create snapshot with keys
        store.put("deleteMe", "value".getBytes(StandardCharsets.UTF_8));
        store.put("keepMe", "value".getBytes(StandardCharsets.UTF_8));
        logger.close();

        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Delete key via WAL
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
//        store = new KVStore.Builder()
//                .setSnapshotter(snapshotter)
//                .setLogHandler(newLogHandler)
//                .setMap(map)
//                .build();

        store = new Restorer().restore(new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler)
                .setMap(map));

        store.delete("deleteMe");
        newLogger.close();

        // Restore
        WALogger restoreLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler restoreLogHandler = new LogHandler(restoreLogger);
        KVStore.Builder restoreBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(restoreLogHandler);

        KVStore restored = new Restorer().restore(restoreBuilder);

        // Verify deleted key is not searchable
        List<KVMap.Node> deleteResult = restored.search("delete");
        assertTrue(deleteResult.isEmpty(), "Key deleted in WAL should not be searchable");

        List<KVMap.Node> keepResult = restored.search("keep");
        assertEquals(1, keepResult.size(), "Other keys should still be searchable");

        if (restoreLogger != null) {
            restoreLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithReaddedKey_keyFound() throws Exception {
        // Create snapshot
        store.put("key1", "original".getBytes(StandardCharsets.UTF_8));
        logger.close();

        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Delete and re-add via WAL
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
//        store = new KVStore.Builder()
//                .setSnapshotter(snapshotter)
//                .setLogHandler(newLogHandler)
//                .setMap(map)
//                .build();

        store = new Restorer().restore(new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler)
                .setMap(map));

        store.delete("key1");
        store.put("key1", "readded".getBytes(StandardCharsets.UTF_8));
        newLogger.close();

        // Restore
        WALogger restoreLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler restoreLogHandler = new LogHandler(restoreLogger);
        KVStore.Builder restoreBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(restoreLogHandler);

        KVStore restored = new Restorer().restore(restoreBuilder);

        // Verify re-added key is searchable
        List<KVMap.Node> result = restored.search("key1");
        assertEquals(1, result.size());
        assertArrayEquals("readded".getBytes(StandardCharsets.UTF_8), result.get(0).value());

        if (restoreLogger != null) {
            restoreLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithEmptyKey_handlesCorrectly() throws Exception {
        // Add empty key
        store.put("", "emptyKeyValue".getBytes(StandardCharsets.UTF_8));
        store.put("normalKey", "normalValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify empty key is handled
        List<KVMap.Node> result = restored.search("");
        assertTrue(result.size() >= 1, "Empty prefix should return at least the normal key");

        KVMap.Node emptyNode = restored.get("");
        assertNotNull(emptyNode, "Empty key should be retrievable");
        assertArrayEquals("emptyKeyValue".getBytes(StandardCharsets.UTF_8), emptyNode.value());

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithVeryLongKey_handlesCorrectly() throws Exception {
        // Create very long key
        StringBuilder longKeyBuilder = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            longKeyBuilder.append("a");
        }
        String longKey = longKeyBuilder.toString();

        store.put(longKey, "longKeyValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify long key is searchable
        List<KVMap.Node> result = restored.search(longKey);
        assertEquals(1, result.size());
        assertEquals(longKey, result.get(0).key());

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithSpecialCharacters_handlesCorrectly() throws Exception {
        // Add keys with special characters
        store.put("key-with-dashes", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key_with_underscores", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key.with.dots", "value3".getBytes(StandardCharsets.UTF_8));
        store.put("key/with/slashes", "value4".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify special character keys are searchable
        List<KVMap.Node> dashResult = restored.search("key-");
        assertEquals(1, dashResult.size());

        List<KVMap.Node> underscoreResult = restored.search("key_");
        assertEquals(1, underscoreResult.size());

        List<KVMap.Node> dotResult = restored.search("key.");
        assertEquals(1, dotResult.size());

        List<KVMap.Node> slashResult = restored.search("key/");
        assertEquals(1, slashResult.size());

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithUnicodeKeys_handlesCorrectly() throws Exception {
        // Add unicode keys
        store.put("café", "coffee".getBytes(StandardCharsets.UTF_8));
        store.put("naïve", "innocent".getBytes(StandardCharsets.UTF_8));
        store.put("日本語", "japanese".getBytes(StandardCharsets.UTF_8));
        store.put("中文", "chinese".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify unicode keys are searchable
        List<KVMap.Node> cafeResult = restored.search("caf");
        assertEquals(1, cafeResult.size());
        assertEquals("café", cafeResult.get(0).key());

        List<KVMap.Node> japaneseResult = restored.search("日本");
        assertEquals(1, japaneseResult.size());
        assertEquals("日本語", japaneseResult.get(0).key());

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterRestoreWithBinaryValues_handlesCorrectly() throws Exception {
        // Add keys with binary values
        byte[] binaryValue1 = new byte[]{0, 1, 2, 3, (byte) 255, (byte) 128};
        byte[] binaryValue2 = new byte[1000];
        for (int i = 0; i < binaryValue2.length; i++) {
            binaryValue2[i] = (byte) (i % 256);
        }

        store.put("binary1", binaryValue1);
        store.put("binary2", binaryValue2);
        logger.close();

        // Create snapshot
        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath);

        // Restore
        WALogger newLogger = new WALogger(logsFolder.resolve("app.log"));
        LogHandler newLogHandler = new LogHandler(newLogger);
        KVStore.Builder newBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(newLogHandler);

        KVStore restored = new Restorer().restore(newBuilder);

        // Verify binary values are preserved
        List<KVMap.Node> result = restored.search("binary");
        assertEquals(2, result.size());

        KVMap.Node node1 = restored.get("binary1");
        assertArrayEquals(binaryValue1, node1.value());

        KVMap.Node node2 = restored.get("binary2");
        assertArrayEquals(binaryValue2, node2.value());

        if (newLogger != null) {
            newLogger.close();
        }
    }

    @Test
    void search_afterMultipleSnapshotRestore_maintainsCorrectState() throws Exception {
        // First snapshot
        store.put("batch1_key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("batch1_key2", "value2".getBytes(StandardCharsets.UTF_8));
        logger.close();

        store.setSnapshotter(snapshotter);
        KVMap map = getMap(store);
        Path fpath1 = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath1);

        // Add more data and create second snapshot
        WALogger logger2 = new WALogger(logsFolder.resolve("app2.log"));
        LogHandler logHandler2 = new LogHandler(logger2);
//        store = new KVStore.Builder()
//                .setSnapshotter(snapshotter)
//                .setLogHandler(logHandler2)
//                .setMap(map)
//                .build();

        store = new Restorer().restore(new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(logHandler2)
                .setMap(map));

        store.put("batch2_key1", "value3".getBytes(StandardCharsets.UTF_8));
        store.delete("batch1_key1");
        logger2.close();

        map = getMap(store);
        Path fpath2 = snapshotter.getDir().resolve(store.getLogHandler().getLogId() + ".snapshot");
        snapshotter.snapshot(map, fpath2);

        // Restore from latest snapshot
        WALogger restoreLogger = new WALogger(logsFolder.resolve("app2.log"));
        LogHandler restoreLogHandler = new LogHandler(restoreLogger);
        KVStore.Builder restoreBuilder = new KVStore.Builder()
                .setSnapshotter(snapshotter)
                .setLogHandler(restoreLogHandler);

        KVStore restored = new Restorer().restore(restoreBuilder);

        // Verify state reflects both batches
        List<KVMap.Node> batch1Result = restored.search("batch1");
        assertEquals(1, batch1Result.size(), "batch1_key1 was deleted");
        assertEquals("batch1_key2", batch1Result.get(0).key());

        List<KVMap.Node> batch2Result = restored.search("batch2");
        assertEquals(1, batch2Result.size());

        if (restoreLogger != null) {
            restoreLogger.close();
        }
    }

    @Test
    void search_verifyTrieMapConsistency_afterOperations() throws Exception {
        // Add keys
        store.put("test1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("test2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("other", "value3".getBytes(StandardCharsets.UTF_8));

        // Verify trie and map are consistent
        List<KVMap.Node> searchResult = store.search("test");
        assertEquals(2, searchResult.size());

        // Delete a key
        store.delete("test1");

        // Verify trie is updated
        searchResult = store.search("test");
        assertEquals(1, searchResult.size());
        assertNull(store.get("test1"));

        // Update a key
        store.put("test2", "updatedValue".getBytes(StandardCharsets.UTF_8));

        // Verify trie still works and value is updated
        searchResult = store.search("test");
        assertEquals(1, searchResult.size());
        assertArrayEquals("updatedValue".getBytes(StandardCharsets.UTF_8), searchResult.get(0).value());
    }
}
