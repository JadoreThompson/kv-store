package com.zenz.kvstore;

//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.io.TempDir;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class KVStoreTest {
//    private Path logsFolder;
//    private Path snapshotsFolder;
//
//    private KVStore store;
//
//    @BeforeEach
//    void setUp() throws IOException {
//        logsFolder = Files.createTempDirectory("tmp-logs-");
//        snapshotsFolder = Files.createTempDirectory("tmp-snapshots-");
//        store = new KVStore(logsFolder.toString(), false);
//    }
//
//    @AfterEach
//    void tearDown() throws IOException {
//        if (store.logger != null) {
//            store.logger.close();
//        }
//
//        logsFolder.toFile().delete();
//        snapshotsFolder.toFile().delete();
//    }
//
//    // --- put / get ---
//
//    @Test
//    void put_thenGet_returnsNode() throws IOException {
//        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
//        store.put("key1", value);
//
//        KVMap.Node node = store.get("key1");
//
//        assertNotNull(node);
//        assertArrayEquals(value, node.value);
//        assertEquals("key1", node.key);
//    }
//
//    @Test
//    void get_missingKey_returnsNull() throws IOException {
//        KVMap.Node node = store.get("nonexistent");
//        assertNull(node);
//    }
//
//    @Test
//    void put_sameKeyTwice_updatesValue() throws IOException {
//        store.put("key1", "first".getBytes(StandardCharsets.UTF_8));
//        store.put("key1", "second".getBytes(StandardCharsets.UTF_8));
//
//        KVMap.Node node = store.get("key1");
//
//        assertNotNull(node);
//        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value);
//    }
//
//    @Test
//    void put_multipleKeys_allRetrievable() throws IOException {
//        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
//        store.put("age", ByteBuffer.allocate(4).putInt(30).array());
//        store.put("city", "london".getBytes(StandardCharsets.UTF_8));
//
//        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8), store.get("name").value);
//        assertEquals(30, ByteBuffer.wrap(store.get("age").value).getInt());
//        assertArrayEquals("london".getBytes(StandardCharsets.UTF_8), store.get("city").value);
//    }
//
//    // --- byte[] values ---
//
//    @Test
//    void put_integerAsBytes_retrievesCorrectly() throws IOException {
//        byte[] value = ByteBuffer.allocate(4).putInt(99).array();
//        store.put("count", value);
//
//        KVMap.Node node = store.get("count");
//
//        assertNotNull(node);
//        assertEquals(99, ByteBuffer.wrap(node.value).getInt());
//    }
//
//    @Test
//    void put_emptyByteArray_retrievesCorrectly() throws IOException {
//        store.put("empty", new byte[0]);
//
//        KVMap.Node node = store.get("empty");
//
//        assertNotNull(node);
//        assertArrayEquals(new byte[0], node.value);
//    }
//
//    // --- large number of entries ---
//
//    @Test
//    void put_manyEntries_allRetrievable() throws IOException {
//        int count = 500;
//        for (int i = 0; i < count; i++) {
//            store.put("key_" + i, ByteBuffer.allocate(4).putInt(i).array());
//        }
//
//        for (int i = 0; i < count; i++) {
//            KVMap.Node node = store.get("key_" + i);
//            assertNotNull(node, "Expected node for key_" + i);
//            assertEquals(i, ByteBuffer.wrap(node.value).getInt());
//        }
//    }
//
//    // --- WAL logging ---
//
//    @Test
//    void put_logsOperationToWAL() throws IOException {
//        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
//        store.logger.close();
//
//        File logFile = logsFolder.resolve("0.log").toFile();
//        String walContents = Files.readString(logFile.toPath());
//
//        assertTrue(walContents.contains("PUT"), "WAL should contain PUT operation");
//        assertTrue(walContents.contains("walKey"), "WAL should contain the key");
//    }
//
//    @Test
//    void get_logsOperationToWAL() throws IOException {
//        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
//        store.get("walKey");
//        store.logger.close();
//
//        File logFile = logsFolder.resolve("0.log").toFile();
//        String walContents = Files.readString(logFile.toPath());
//
//        assertTrue(walContents.contains("GET"), "WAL should contain GET operation");
//        assertTrue(walContents.contains("walKey"), "WAL should contain the key");
//    }
//
//    @Test
//    void put_andGet_bothLoggedToWAL() throws IOException {
//        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
//        store.get("name");
//        store.logger.close();
//
//        File logFile = logsFolder.resolve("0.log").toFile();
//        String walContents = Files.readString(logFile.toPath());
//        String[] lines = walContents.strip().split("\n");
//
//        assertEquals(2, lines.length, "WAL should have exactly 2 entries");
//        assertTrue(lines[0].contains("PUT"), "First entry should be PUT");
//        assertTrue(lines[1].contains("GET"), "Second entry should be GET");
//    }
//
//    @Test
//    void multipleOperations_allLoggedToWAL() throws IOException {
//        store.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
//        store.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
//        store.get("k1");
//        store.get("k2");
//        store.logger.close();
//
//        File logFile = logsFolder.resolve("0.log").toFile();
//        String walContents = Files.readString(logFile.toPath());
//        String[] lines = walContents.strip().split("\n");
//
//        assertEquals(4, lines.length, "WAL should have exactly 4 entries");
//        assertTrue(lines[0].contains("PUT") && lines[0].contains("k1"), "Line 1 should be PUT k1");
//        assertTrue(lines[1].contains("PUT") && lines[1].contains("k2"), "Line 2 should be PUT k2");
//        assertTrue(lines[2].contains("GET") && lines[2].contains("k1"), "Line 3 should be GET k1");
//        assertTrue(lines[3].contains("GET") && lines[3].contains("k2"), "Line 4 should be GET k2");
//    }
//
//    // --- Integration: Snapshotting ---
//
//    @Test
//    void snapshotter_createsSnapshotFromWAL() throws IOException {
//        // Add some data
//        store.put("snapKey1", "snapValue1".getBytes(StandardCharsets.UTF_8));
//        store.put("snapKey2", "snapValue2".getBytes(StandardCharsets.UTF_8));
//        store.logger.close();
//
//        // Create snapshotter and snapshot
//        KVSnapshotter snapshotter = new KVSnapshotter(snapshotsFolder.toString());
//        Path logFile = logsFolder.resolve("0.log");
//        snapshotter.snapshot(logFile.toString());
//
//        // Verify snapshot file exists
//        Path snapshotFile = snapshotsFolder.resolve("0.snapshot");
//        assertTrue(snapshotFile.toFile().exists(), "Snapshot file should be created");
//
//        // Verify snapshot contents
//        String snapshotContents = Files.readString(snapshotFile);
//        assertTrue(snapshotContents.contains("snapKey1"), "Snapshot should contain snapKey1");
//        assertTrue(snapshotContents.contains("snapKey2"), "Snapshot should contain snapKey2");
//    }
//
//    @Test
//    void snapshotter_restoresDataFromSnapshot() throws IOException {
//        // Add some data
//        store.put("restoreKey1", "restoreValue1".getBytes(StandardCharsets.UTF_8));
//        store.put("restoreKey2", "restoreValue2".getBytes(StandardCharsets.UTF_8));
//        store.logger.close();
//
//        // Create snapshot
//        KVSnapshotter snapshotter = new KVSnapshotter(snapshotsFolder.toString());
//        Path logFile = logsFolder.resolve("0.log");
//        snapshotter.snapshot(logFile.toString());
//
//        // Load snapshot
//        Path snapshotFile = snapshotsFolder.resolve("0.snapshot");
//        KVMap map = snapshotter.loadSnapshot(snapshotFile.toString());
//        KVStore restored = new KVStore(logsFolder.toString(), false, map);
//
//        // Verify restored data
//        assertNotNull(restored.get("restoreKey1"), "restoreKey1 should exist in restored store");
//        assertNotNull(restored.get("restoreKey2"), "restoreKey2 should exist in restored store");
//        assertArrayEquals("restoreValue1".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey1").value);
//        assertArrayEquals("restoreValue2".getBytes(StandardCharsets.UTF_8), restored.get("restoreKey2").value);
//
//        restored.logger.close();
//    }
//
//    @Test
//    void snapshotter_roundTrip_preservesData() throws IOException {
//        // Add multiple entries
//        for (int i = 0; i < 10; i++) {
//            store.put("roundtripKey" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
//        }
//        store.logger.close();
//
//        // Create and load snapshot
//        KVSnapshotter snapshotter = new KVSnapshotter(snapshotsFolder.toString());
//        Path logFile = logsFolder.resolve("0.log");
//        snapshotter.snapshot(logFile.toString());
//
//        Path snapshotFile = snapshotsFolder.resolve("0.snapshot");
//        KVMap map = snapshotter.loadSnapshot(snapshotFile.toString());
//        KVStore restored = new KVStore(logsFolder.toString(), false, map);
//
//        // Verify all entries
//        for (int i = 0; i < 10; i++) {
//            KVMap.Node node = restored.get("roundtripKey" + i);
//            assertNotNull(node, "Key roundtripKey" + i + " should exist");
//            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
//        }
//
//        restored.logger.close();
//    }
//}


class KVStoreTest {
}