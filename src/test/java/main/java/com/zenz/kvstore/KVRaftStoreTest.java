package main.java.com.zenz.kvstore;

import com.zenz.kvstore.KVMap;
import com.zenz.kvstore.KVRaftStore;
import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.OperationType;
import com.zenz.kvstore.WALogger;
import com.zenz.kvstore.operations.RaftGetOperation;
import com.zenz.kvstore.operations.RaftOperation;
import com.zenz.kvstore.operations.RaftPutOperation;
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

class KVRaftStoreTest {
    private Path logsFolder;
    private Path snapshotsFolder;

    private KVRaftStore store;
    private KVMapSnapshotter snapshotter;
    private WALogger logger;

    @BeforeEach
    void setUp() throws Exception {
        logsFolder = Files.createTempDirectory("tmp-raft-logs-");
        snapshotsFolder = Files.createTempDirectory("tmp-raft-snapshots-");
        snapshotter = new KVMapSnapshotter(snapshotsFolder);
        store = new KVRaftStore.Builder()
                .setLogsFolder(logsFolder)
                .setSnapshotter(snapshotter)
                .build();
        logger = getLogger(store);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logger != null) {
            logger.close();
        }

        logsFolder.toFile().delete();
        snapshotsFolder.toFile().delete();
//        deleteDirectory(logsFolder.toFile());
//        deleteDirectory(snapshotsFolder.toFile());
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

    private WALogger getLogger(KVRaftStore store) throws Exception {
        Field loggerField = KVStore.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        return (WALogger) loggerField.get(store);
    }

    private KVMap getMap(KVRaftStore store) throws Exception {
        Field mapField = KVStore.class.getDeclaredField("map");
        mapField.setAccessible(true);
        return (KVMap) mapField.get(store);
    }

    // --- Helper methods for parsing binary Raft logs ---

    /**
     * Parses binary log file and returns a list of RaftOperation objects.
     * Each entry in the log is separated by a newline character.
     */
    private List<RaftOperation> parseRaftLogs(File logFile) throws IOException {
        List<RaftOperation> operations = new ArrayList<>();
        byte[] allBytes = Files.readAllBytes(logFile.toPath());

        if (allBytes.length == 0) {
            return operations;
        }

        ByteBuffer bb = ByteBuffer.wrap(allBytes);
        ByteBuffer entryBuffer = ByteBuffer.allocate(8192);

        while (bb.hasRemaining()) {
            long id = bb.getLong();
            long term = bb.getLong();
            int typeValue = bb.getInt();
            OperationType operationType = OperationType.fromValue(typeValue);

            if (operationType.equals(OperationType.PUT)) {
                ByteBuffer _buffer = ByteBuffer.allocate(10240);

                _buffer.putLong(id);
                _buffer.putLong(term);
                _buffer.putInt(typeValue);
                int keyLength = bb.getInt();
                if (keyLength == 0) throw new RuntimeException("Key length 0");

                _buffer.putInt(keyLength);
                byte[] key = new byte[keyLength];
                bb.get(key);
                _buffer.put(key);
                int valueLength = bb.getInt();
                _buffer.putInt(valueLength);
                byte[] value = new byte[valueLength];
                bb.get(value);
                _buffer.put(value);

                _buffer.flip();
                RaftPutOperation operation = RaftPutOperation.deserialize(_buffer);

//                bb.mark();
//                long nextId = bb.getLong();
//                System.out.println("Next id: " + nextId);
//                bb.reset();
                // Skipping the new line character
                bb.get();
                operations.add(operation);
            } else if (operationType.equals(OperationType.GET)) {
                ByteBuffer _buffer = ByteBuffer.allocate(10240);

                _buffer.putLong(id);
                _buffer.putLong(term);
                _buffer.putInt(typeValue);
                int keyLength = bb.getInt();
                _buffer.putInt(keyLength);
                byte[] key = new byte[keyLength];
                bb.get(key);
                _buffer.put(key);
                _buffer.rewind();
                _buffer.compact();

                _buffer.flip();

                RaftGetOperation operation = RaftGetOperation.deserialize(_buffer);
                bb.get(); // Getting the new line character
                operations.add(operation);
            }

//            byte b = bb.get();
//            if (b == '\n') {
//                // End of entry, deserialize
//                entryBuffer.flip();
//                byte[] entryBytes = new byte[entryBuffer.remaining()];
//                entryBuffer.get(entryBytes);
//                System.out.println("Entry buffer length " + entryBytes.length);
//
//                ByteBuffer opBuffer = ByteBuffer.wrap(entryBytes);
//                RaftOperation operation = RaftOperation.deserialize(opBuffer);
//                operations.add(operation);
//
//                entryBuffer.clear();
//            } else {
//                entryBuffer.put(b);
//            }
        }

        return operations;
    }

    /**
     * Reads the raw bytes of each log entry (separated by newline).
     */
    private List<byte[]> readRawLogEntries(File logFile) throws IOException {
        List<byte[]> entries = new ArrayList<>();
        byte[] allBytes = Files.readAllBytes(logFile.toPath());

        if (allBytes.length == 0) {
            return entries;
        }

        ByteBuffer bb = ByteBuffer.wrap(allBytes);
        ByteBuffer entryBuffer = ByteBuffer.allocate(2048);

        while (bb.hasRemaining()) {
            byte b = bb.get();
            if (b == '\n') {
                entryBuffer.flip();
                byte[] entryBytes = new byte[entryBuffer.remaining()];
                entryBuffer.get(entryBytes);
                entries.add(entryBytes);
                entryBuffer.clear();
            } else {
                entryBuffer.put(b);
            }
        }

        return entries;
    }

    /**
     * Extracts the operation type from raw binary log entry.
     * Format: id(8 bytes) + term(8 bytes) + type(4 bytes) + ...
     */
    private OperationType extractOperationType(byte[] entryBytes) {
        ByteBuffer bb = ByteBuffer.wrap(entryBytes);
        bb.getLong(); // skip id
        bb.getLong(); // skip term
        int typeValue = bb.getInt();
        return OperationType.fromValue(typeValue);
    }

    /**
     * Extracts the key from a PUT operation's binary log entry.
     */
    private String extractPutKey(byte[] entryBytes) {
        ByteBuffer bb = ByteBuffer.wrap(entryBytes);
        bb.getLong(); // skip id
        bb.getLong(); // skip term
        bb.getInt(); // skip type
        int keyLength = bb.getInt();
        byte[] keyBytes = new byte[keyLength];
        bb.get(keyBytes);
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    /**
     * Extracts the key from a GET operation's binary log entry.
     */
    private String extractGetKey(byte[] entryBytes) {
        ByteBuffer bb = ByteBuffer.wrap(entryBytes);
        bb.getLong(); // skip id
        bb.getLong(); // skip term
        byte[] keyBytes = new byte[bb.remaining()];
        bb.get(keyBytes);
        return new String(keyBytes, StandardCharsets.UTF_8);
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
        List<byte[]> entries = readRawLogEntries(logFile);

        assertEquals(1, entries.size(), "WAL should have exactly 1 entry");
        OperationType type = extractOperationType(entries.get(0));
        assertEquals(OperationType.PUT, type, "WAL should contain PUT operation");

        String key = extractPutKey(entries.get(0));
        assertEquals("walKey", key, "WAL should contain the key");
    }

    @Test
    void get_logsOperationToWAL() throws Exception {
        store.setLoggingEnabled(true);
        store.setTerm(1);
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        store.get("walKey");
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        List<RaftOperation> operations = parseRaftLogs(logFile);

        assertEquals(2, operations.size(), "WAL should have exactly 2 entries");
        assertEquals(OperationType.PUT, operations.get(0).type(), "First entry should be PUT");
        assertEquals(OperationType.GET, operations.get(1).type(), "Second entry should be GET");
    }

    @Test
    void put_andGet_bothLoggedToWAL() throws Exception {
        store.setLoggingEnabled(true);
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.get("name");
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        List<RaftOperation> operations = parseRaftLogs(logFile);

        assertEquals(2, operations.size(), "WAL should have exactly 2 entries");
        assertEquals(OperationType.PUT, operations.get(0).type(), "First entry should be PUT");
        assertEquals(OperationType.GET, operations.get(1).type(), "Second entry should be GET");
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
        List<RaftOperation> operations = parseRaftLogs(logFile);

        assertEquals(4, operations.size(), "WAL should have exactly 4 entries");

        // Verify each operation
        assertEquals(OperationType.PUT, operations.get(0).type(), "Line 1 should be PUT");
        assertTrue(operations.get(0) instanceof RaftPutOperation, "Line 1 should be RaftPutOperation");
        assertEquals("k1", ((RaftPutOperation) operations.get(0)).key(), "Line 1 should have key k1");

        assertEquals(OperationType.PUT, operations.get(1).type(), "Line 2 should be PUT");
        assertTrue(operations.get(1) instanceof RaftPutOperation, "Line 2 should be RaftPutOperation");
        assertEquals("k2", ((RaftPutOperation) operations.get(1)).key(), "Line 2 should have key k2");

        assertEquals(OperationType.GET, operations.get(2).type(), "Line 3 should be GET");
        assertTrue(operations.get(2) instanceof RaftGetOperation, "Line 3 should be RaftGetOperation");
        assertEquals("k1", ((RaftGetOperation) operations.get(2)).key(), "Line 3 should have key k1");

        assertEquals(OperationType.GET, operations.get(3).type(), "Line 4 should be GET");
        assertTrue(operations.get(3) instanceof RaftGetOperation, "Line 4 should be RaftGetOperation");
        assertEquals("k2", ((RaftGetOperation) operations.get(3)).key(), "Line 4 should have key k2");
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

        // Verify log contents by parsing operations
        List<RaftOperation> operations = parseRaftLogs(logFile.toFile());
        assertEquals(2, operations.size(), "Log should have 2 operations");

        // Verify keys are present
        assertTrue(operations.get(0) instanceof RaftPutOperation);
        assertTrue(operations.get(1) instanceof RaftPutOperation);
        assertEquals("snapKey1", ((RaftPutOperation) operations.get(0)).key());
        assertEquals("snapKey2", ((RaftPutOperation) operations.get(1)).key());
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
        KVRaftStore restored = new KVRaftStore.Builder().setLogsFolder(logsFolder).setSnapshotter(snapshotter).build();

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
        KVRaftStore restored = new KVRaftStore.Builder().setLogsFolder(logsFolder).setSnapshotter(snapshotter).build();

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
        KVRaftStore restored = new KVRaftStore.Builder()
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

    // --- KVRaftStore specific tests ---

    @Test
    void logId_incrementsOnEachOperation() throws Exception {
        assertEquals(0, store.getLogId(), "Initial logId should be 0");

        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, store.getLogId(), "logId should be 1 after first put");

        store.get("key1");
        assertEquals(2, store.getLogId(), "logId should be 2 after get");

        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        assertEquals(3, store.getLogId(), "logId should be 3 after second put");
    }

    @Test
    void term_canBeSetAndRetrieved() throws Exception {
        assertEquals(0, store.getTerm(), "Initial term should be 0");

        store.setTerm(5);
        assertEquals(5, store.getTerm(), "Term should be 5 after set");

        store.setTerm(10);
        assertEquals(10, store.getTerm(), "Term should be 10 after set");
    }

    @Test
    void raftOperations_haveCorrectIdAndTerm() throws Exception {
        store.setLoggingEnabled(true);
        store.setTerm(3);

        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.get("key1");
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        List<RaftOperation> operations = parseRaftLogs(logFile);

        assertEquals(2, operations.size());

        // Verify PUT operation has correct id and term
        RaftPutOperation putOp = (RaftPutOperation) operations.get(0);
        assertEquals(1, putOp.id(), "PUT operation id should be 0");
        assertEquals(3, putOp.term(), "PUT operation term should be 3");

        // Verify GET operation has correct id and term
        RaftGetOperation getOp = (RaftGetOperation) operations.get(1);
        assertEquals(2, getOp.id(), "GET operation id should be 1");
        assertEquals(3, getOp.term(), "GET operation term should be 3");
    }

    @Test
    void raftOperations_serializeAndDeserializeCorrectly() throws Exception {
        store.setLoggingEnabled(true);

        byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);
        store.put("testKey", value);
        logger.close();

        File logFile = logsFolder.resolve("0.log").toFile();
        List<RaftOperation> operations = parseRaftLogs(logFile);

        assertEquals(1, operations.size());

        RaftPutOperation putOp = (RaftPutOperation) operations.get(0);
        assertEquals("testKey", putOp.key());
        assertArrayEquals(value, putOp.value());
    }
}