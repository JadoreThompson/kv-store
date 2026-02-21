package com.zenz.kvstore;

import com.zenz.kvstore.KVMap;
import com.zenz.kvstore.KVMapSnapshotter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class KVMapSnapshotterTest {

    private Path snapshotFolder;
    private KVMapSnapshotter snapshotter;

    @BeforeEach
    void setUp() throws IOException {
        snapshotFolder = Files.createTempDirectory("tmp-snapshots-");
        snapshotter = new KVMapSnapshotter(snapshotFolder);
        snapshotter.setMultiThreadingEnabled(false);
    }

    @AfterEach
    void tearDown() throws IOException {
        deleteDirectory(snapshotFolder.toFile());
    }

    private void deleteDirectory(java.io.File directory) {
        java.io.File[] files = directory.listFiles();
        if (files != null) {
            for (java.io.File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    // --- Unit Tests ---

    @Test
    void snapshot_createsFile() throws IOException {
        // Create a map with data
        KVMap map = new KVMap();
        map.put("testkey", "testvalue".getBytes(StandardCharsets.UTF_8));

        // Run snapshot
        snapshotter.snapshot(map);

        // Verify snapshot file was created
        java.io.File[] files = snapshotFolder.toFile().listFiles();
        assertNotNull(files, "Snapshot folder should not be null");
        assertTrue(files.length > 0, "Snapshot file should be created");
    }

    @Test
    void snapshot_containsKVPair() throws IOException {
        // Create a map with data
        KVMap map = new KVMap();
        map.put("mykey", "myvalue".getBytes(StandardCharsets.UTF_8));

        // Run snapshot
        snapshotter.snapshot(map);

        // Read snapshot file
        File[] files = snapshotFolder.toFile().listFiles();
        assertNotNull(files, "Snapshot folder should not be null");
        assertTrue(files.length > 0, "Snapshot file should be created");

        String contents = Files.readString(files[0].toPath());
        assertTrue(contents.contains("mykey"), "Snapshot should contain the key");
    }

    @Test
    void loadSnapshot_restoresData() throws IOException {
        // Create a snapshot file manually
        Path snapshotFile = snapshotFolder.resolve("0.snapshot");
        StringBuilder sb = new StringBuilder();
        sb.append("===HEADER START===\n");
        sb.append("1\n");
        sb.append("===HEADER END===\n");
        sb.append("===KV START===\n");
        sb.append("key1 value1\n");
        sb.append("key2 value2\n");
        sb.append("===KV END===\n");
        Files.writeString(snapshotFile, sb.toString());

        // Load the snapshot using the public API
        KVMap map = snapshotter.loadSnapshot();

        // Verify data was restored
        assertNotNull(map.get("key1"), "key1 should exist");
        assertNotNull(map.get("key2"), "key2 should exist");
    }

    @Test
    void roundTrip_saveAndLoad() throws IOException {
        // Create a map with data
        KVMap map = new KVMap();
        map.put("alpha", "beta".getBytes(StandardCharsets.UTF_8));
        map.put("gamma", "delta".getBytes(StandardCharsets.UTF_8));

        // Create snapshot
        snapshotter.snapshot(map);

        // Load the snapshot
        KVMap restoredMap = snapshotter.loadSnapshot();

        // Verify data
        assertNotNull(restoredMap.get("alpha"), "alpha key should exist");
        assertNotNull(restoredMap.get("gamma"), "gamma key should exist");
        assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), restoredMap.get("alpha").value);
        assertArrayEquals("delta".getBytes(StandardCharsets.UTF_8), restoredMap.get("gamma").value);
    }

    @Test
    void snapshot_handlesMultipleEntries() throws IOException {
        // Create a map with multiple entries
        KVMap map = new KVMap();
        map.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        map.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        map.put("k3", "v3".getBytes(StandardCharsets.UTF_8));

        // Create snapshot
        snapshotter.snapshot(map);

        // Load the snapshot
        KVMap restoredMap = snapshotter.loadSnapshot();

        assertNotNull(restoredMap.get("k1"));
        assertNotNull(restoredMap.get("k2"));
        assertNotNull(restoredMap.get("k3"));
    }

    @Test
    void snapshot_handlesEmptyMap() throws IOException {
        // Create an empty map
        KVMap map = new KVMap();

        // Run snapshot - should not throw
        assertDoesNotThrow(() -> snapshotter.snapshot(map));
    }

    @Test
    void loadSnapshot_handlesMultipleEntries() throws IOException {
        // Create a snapshot file with multiple entries
        Path snapshotFile = snapshotFolder.resolve("0.snapshot");
        StringBuilder sb = new StringBuilder();
        sb.append("===HEADER START===\n");
        sb.append("1\n");
        sb.append("===HEADER END===\n");
        sb.append("===KV START===\n");
        for (int i = 0; i < 10; i++) {
            sb.append("key" + i + " value" + i + "\n");
        }
        sb.append("===KV END===\n");
        Files.writeString(snapshotFile, sb.toString());

        // Load the snapshot using the public API
        KVMap map = snapshotter.loadSnapshot();

        // Verify all entries
        for (int i = 0; i < 10; i++) {
            KVMap.Node node = map.get("key" + i);
            assertNotNull(node, "key" + i + " should exist");
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
        }
    }

    @Test
    void loadSnapshot_returnsNullWhenNoSnapshots() throws IOException {
        // Don't create any snapshot files
        KVMap map = snapshotter.loadSnapshot();
        assertNull(map, "Should return null when no snapshots exist");
    }

    @Test
    void getFolderPath_returnsCorrectPath() {
        assertEquals(snapshotFolder, snapshotter.getFolderPath());
    }
}