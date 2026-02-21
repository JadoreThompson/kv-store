package com.zenz.kvstore;

//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.io.TempDir;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class KVSnapshotterTest {
//
//    @TempDir
//    Path tempDir;
//
//    private KVSnapshotter snapshotter;
//    private Path snapshotFolder;
//
//    @BeforeEach
//    void setUp() throws IOException {
//        snapshotFolder = tempDir.resolve("snapshots");
//        Files.createDirectories(snapshotFolder);
//        snapshotter = new KVSnapshotter(snapshotFolder.toString());
//    }
//
//    @AfterEach
//    void tearDown() throws IOException {
//        // Cleanup handled by @TempDir
//    }
//
//    // --- Unit Tests ---
//
//    @Test
//    void snapshot_createsFile() throws IOException {
//        // Create a log file with a PUT operation
//        Path logFile = tempDir.resolve("0.log");
//        FileWriter writer = new FileWriter(logFile.toFile());
//        writer.write("123 PUT testkey testvalue");
//        writer.flush();
//        writer.close();
//
//        // Run snapshot
//        snapshotter.snapshot(logFile.toString());
//
//        // Verify snapshot file was created
//        Path snapshotFile = snapshotFolder.resolve("0.snapshot");
//        assertTrue(snapshotFile.toFile().exists(), "Snapshot file should be created");
//    }
//
//    @Test
//    void snapshot_containsKVPair() throws IOException {
//        // Create a log file with a PUT operation
//        Path logFile = tempDir.resolve("0.log");
//        Files.writeString(logFile, "123 PUT mykey myvalue\n");
//
//        // Run snapshot
//        snapshotter.snapshot(logFile.toString());
//
//        // Read snapshot file
//        Path snapshotFile = snapshotFolder.resolve("0.snapshot");
//        String contents = Files.readString(snapshotFile);
//
//        assertTrue(contents.contains("mykey"), "Snapshot should contain the key");
//    }
//
//    @Test
//    void loadSnapshotV2_restoresData() throws IOException {
//        // Create a snapshot file manually
//        Path snapshotFile = snapshotFolder.resolve("test.snapshot");
//        StringBuilder sb = new StringBuilder();
//        sb.append("===HEADER START===\n");
//        sb.append("1\n");
//        sb.append("===HEADER END===\n");
//        sb.append("===KV START===\n");
//        sb.append("key1 value1\n");
//        sb.append("key2 value2\n");
//        sb.append("===KV END===\n");
//        Files.writeString(snapshotFile, sb.toString());
//
//        // Load the snapshot using V2
//        KVMap map = snapshotter.loadSnapshot(snapshotFile.toString());
//
//        // Construct store manually with the loaded map
//        KVStore store = new KVStore(tempDir.resolve("store").toString(), false, map);
//
//        // Verify data was restored
//        assertNotNull(store.get("key1"), "key1 should exist");
//        assertNotNull(store.get("key2"), "key2 should exist");
//    }
//
//    @Test
//    void roundTrip_saveAndLoad() throws IOException {
//        // Create a log file with operations
//        Path logFile = tempDir.resolve("0.log");
//        Files.writeString(logFile, "1 PUT alpha beta\n2 PUT gamma delta\n");
//
//        // Create snapshot
//        snapshotter.snapshot(logFile.toString());
//
//        // Load the snapshot using V2
//        Path snapshotFile = snapshotFolder.resolve("0.snapshot");
//        KVMap map = snapshotter.loadSnapshot(snapshotFile.toString());
//
//        // Construct store manually with the loaded map
//        KVStore restored = new KVStore(tempDir.resolve("restored").toString(), false, map);
//
//        // Verify data
//        assertNotNull(restored.get("alpha"), "alpha key should exist");
//        assertNotNull(restored.get("gamma"), "gamma key should exist");
//        assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), restored.get("alpha").value);
//        assertArrayEquals("delta".getBytes(StandardCharsets.UTF_8), restored.get("gamma").value);
//    }
//
//    @Test
//    void snapshot_handlesMultipleOperations() throws IOException {
//        // Create a log file with multiple operations
//        Path logFile = tempDir.resolve("0.log");
//        StringBuilder logContent = new StringBuilder();
//        logContent.append("1 PUT k1 v1\n");
//        logContent.append("2 PUT k2 v2\n");
//        logContent.append("3 PUT k3 v3\n");
//        logContent.append("4 GET k1\n");  // GET should not affect data
//        Files.writeString(logFile, logContent.toString());
//
//        // Create snapshot
//        snapshotter.snapshot(logFile.toString());
//
//        // Load using V2 and construct store manually
//        Path snapshotFile = snapshotFolder.resolve("0.snapshot");
//        KVMap map = snapshotter.loadSnapshot(snapshotFile.toString());
//        KVStore restored = new KVStore(tempDir.resolve("restored").toString(), false, map);
//
//        assertNotNull(restored.get("k1"));
//        assertNotNull(restored.get("k2"));
//        assertNotNull(restored.get("k3"));
//    }
//
//    @Test
//    void snapshot_handlesEmptyLog() throws IOException {
//        // Create an empty log file
//        Path logFile = tempDir.resolve("0.log");
//        Files.writeString(logFile, "");
//
//        // Run snapshot - should not throw
//        assertDoesNotThrow(() -> snapshotter.snapshot(logFile.toString()));
//    }
//
//    @Test
//    void loadSnapshotV2_handlesMultipleEntries() throws IOException {
//        // Create a snapshot file with multiple entries
//        Path snapshotFile = snapshotFolder.resolve("multi.snapshot");
//        StringBuilder sb = new StringBuilder();
//        sb.append("===HEADER START===\n");
//        sb.append("1\n");
//        sb.append("===HEADER END===\n");
//        sb.append("===KV START===\n");
//        for (int i = 0; i < 10; i++) {
//            sb.append("key" + i + " value" + i + "\n");
//        }
//        sb.append("===KV END===\n");
//        Files.writeString(snapshotFile, sb.toString());
//
//        // Load using V2
//        KVMap map = snapshotter.loadSnapshot(snapshotFile.toString());
//        KVStore store = new KVStore(tempDir.resolve("store").toString(), false, map);
//
//        // Verify all entries
//        for (int i = 0; i < 10; i++) {
//            KVMap.Node node = store.get("key" + i);
//            assertNotNull(node, "key" + i + " should exist");
//            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
//        }
//    }


class KVSnapshotterTest {
}