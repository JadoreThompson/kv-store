package com.zenz.kvstore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class KVMapSnapshotterTest {

    private Path snapshotFolder;
    private KVMapSnapshotter snapshotter;

    @BeforeEach
    void setUp() throws IOException {
        snapshotFolder = Files.createTempDirectory("tmp-snapshots-");
        snapshotter = new KVMapSnapshotter(snapshotFolder);
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
        snapshotter.snapshot(map, snapshotter.getDir().resolve("snapshot"));

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
        snapshotter.snapshot(map, snapshotter.getDir().resolve("snapshot"));

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

        KVMap map = new KVMap();
        map.put("key1", "value2".getBytes(StandardCharsets.UTF_8));
        map.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        Files.createFile(snapshotFile);
        try (FileChannel channel = FileChannel.open(snapshotFile, StandardOpenOption.WRITE)) {
            snapshotter.writeNodes(channel, map);
        }

        // Load the snapshot using the public API
        KVMap restored = snapshotter.loadSnapshot(snapshotFile);

        // Verify data was restored
        assertNotNull(restored.get("key1"), "key1 should exist");
        assertNotNull(restored.get("key2"), "key2 should exist");
    }

    @Test
    void roundTrip_saveAndLoad() throws IOException {
        // Create a map with data
        KVMap map = new KVMap();
        map.put("alpha", "beta".getBytes(StandardCharsets.UTF_8));
        map.put("gamma", "delta".getBytes(StandardCharsets.UTF_8));

        // Create snapshot
        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        // Load the snapshot
        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

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
        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        // Load the snapshot
        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        assertNotNull(restoredMap.get("k1"));
        assertNotNull(restoredMap.get("k2"));
        assertNotNull(restoredMap.get("k3"));
    }

    @Test
    void snapshot_handlesEmptyMap() throws IOException {
        // Create an empty map
        KVMap map = new KVMap();

        // Run snapshot - should not throw
        Path fpath = snapshotter.getDir().resolve("snapshot");
        assertDoesNotThrow(() -> snapshotter.snapshot(map, fpath));
    }

    @Test
    void loadSnapshot_handlesMultipleEntries() throws IOException {
        // Create a snapshot file with multiple entries
        KVMap map = new KVMap();
        for (int i = 0; i < 10; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }
        Path snapshotFile = snapshotFolder.resolve("0.snapshot");
        snapshotter.snapshot(map, snapshotFile);

        // Load the snapshot using the public API
        KVMap restored = snapshotter.loadSnapshot(snapshotFile);

        // Verify all entries
        for (int i = 0; i < 10; i++) {
            KVMap.Node node = restored.get("key" + i);
            assertNotNull(node, "key" + i + " should exist");
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value);
        }
    }

    @Test
    void loadSnapshot_returnsNullWhenNoSnapshots() throws IOException {
        // Don't create any snapshot files
        Path fpath = snapshotter.getDir().resolve("snapshot");
        KVMap map = snapshotter.loadSnapshot(fpath);
        assertNull(map, "Should return null when no snapshots exist");
    }

    @Test
    void getFolderPath_returnsCorrectPath() {
        assertEquals(snapshotFolder, snapshotter.getDir());
    }
}