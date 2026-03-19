package com.zenz.kvstore.server;

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

    private Path snapshotsDir;
    private KVMapSnapshotter snapshotter;

    @BeforeEach
    void setUp() throws IOException {
        snapshotsDir = Files.createTempDirectory("tmp-snapshots-");
        snapshotter = new KVMapSnapshotter(snapshotsDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        deleteDirectory(snapshotsDir.toFile());
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

    @Test
    void snapshot_createsFile() throws IOException {
        KVMap map = new KVMap();
        map.put("testkey", "testvalue".getBytes(StandardCharsets.UTF_8));

        snapshotter.snapshot(map, snapshotter.getDir().resolve("snapshot"));

        // Verify snapshot file was created
        java.io.File[] files = snapshotsDir.toFile().listFiles();
        assertNotNull(files, "Snapshot folder should not be null");
        assertTrue(files.length > 0, "Snapshot file should be created");
    }

    @Test
    void snapshot_containsKVPair() throws IOException {
        KVMap map = new KVMap();
        map.put("mykey", "myvalue".getBytes(StandardCharsets.UTF_8));

        snapshotter.snapshot(map, snapshotter.getDir().resolve("snapshot"));

        File[] files = snapshotsDir.toFile().listFiles();
        assertNotNull(files, "Snapshot folder should not be null");
        assertTrue(files.length > 0, "Snapshot file should be created");

        String contents = Files.readString(files[0].toPath());
        assertTrue(contents.contains("mykey"), "Snapshot should contain the key");
    }

    @Test
    void loadSnapshot_restoresData() throws IOException {
        Path snapshotFile = snapshotsDir.resolve("0.snapshot");

        KVMap map = new KVMap();
        map.put("key1", "value2".getBytes(StandardCharsets.UTF_8));
        map.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        Files.createFile(snapshotFile);
        try (FileChannel channel = FileChannel.open(snapshotFile, StandardOpenOption.WRITE)) {
            snapshotter.writeNodes(channel, map);
        }

        KVMap restored = snapshotter.loadSnapshot(snapshotFile);

        // Verify data was restored
        assertNotNull(restored.get("key1"), "key1 should exist");
        assertNotNull(restored.get("key2"), "key2 should exist");
    }

    @Test
    void roundTrip_saveAndLoad() throws IOException {
        KVMap map = new KVMap();
        map.put("alpha", "beta".getBytes(StandardCharsets.UTF_8));
        map.put("gamma", "delta".getBytes(StandardCharsets.UTF_8));

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Verify data
        assertNotNull(restoredMap.get("alpha"), "alpha key should exist");
        assertNotNull(restoredMap.get("gamma"), "gamma key should exist");
        assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), restoredMap.get("alpha").value());
        assertArrayEquals("delta".getBytes(StandardCharsets.UTF_8), restoredMap.get("gamma").value());
    }

    @Test
    void snapshot_handlesMultipleEntries() throws IOException {
        KVMap map = new KVMap();
        map.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        map.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        map.put("k3", "v3".getBytes(StandardCharsets.UTF_8));

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Verify entries
        assertNotNull(restoredMap.get("k1"));
        assertNotNull(restoredMap.get("k2"));
        assertNotNull(restoredMap.get("k3"));
    }

    @Test
    void snapshot_handlesEmptyMap() throws IOException {
        KVMap map = new KVMap();

        // Run snapshot - should not throw
        Path fpath = snapshotter.getDir().resolve("snapshot");
        assertDoesNotThrow(() -> snapshotter.snapshot(map, fpath));
    }

    @Test
    void loadSnapshot_handlesMultipleEntries() throws IOException {
        KVMap map = new KVMap();
        for (int i = 0; i < 10; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }
        System.out.println(2);
        Path snapshotFile = snapshotsDir.resolve("0.snapshot");
        System.out.println(3);
        snapshotter.snapshot(map, snapshotFile);

        System.out.println(4);
        KVMap restored = snapshotter.loadSnapshot(snapshotFile);

        // Verify all entries
        System.out.println(5);
        for (int i = 0; i < 10; i++) {
            KVMap.Node node = restored.get("key" + i);
            assertNotNull(node, "key" + i + " should exist");
            assertArrayEquals(("value" + i).getBytes(StandardCharsets.UTF_8), node.value());
        }
    }

    @Test
    void loadSnapshot_returnsNullWhenNoSnapshots() throws IOException {
        Path fpath = snapshotter.getDir().resolve("snapshot");
        KVMap map = snapshotter.loadSnapshot(fpath);
        assertNull(map, "Should return null when no snapshots exist");
    }

    @Test
    void getFolderPath_returnsCorrectPath() {
        assertEquals(snapshotsDir, snapshotter.getDir());
    }

    // --- DELETE Unit Tests ---

    @Test
    void snapshot_afterDelete_excludesDeletedKey() throws IOException {
        KVMap map = new KVMap();
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        map.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        map.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        // Delete key2
        map.remove("key2");

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Verify deleted key is not in snapshot
        assertNotNull(restoredMap.get("key1"), "key1 should exist");
        assertNull(restoredMap.get("key2"), "key2 should be deleted");
        assertNotNull(restoredMap.get("key3"), "key3 should exist");
    }

    @Test
    void snapshot_afterDeleteAllKeys_producesEmptySnapshot() throws IOException {
        KVMap map = new KVMap();
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        map.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        // Delete all keys
        map.remove("key1");
        map.remove("key2");

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Verify map is empty
        assertNull(restoredMap.get("key1"), "key1 should not exist");
        assertNull(restoredMap.get("key2"), "key2 should not exist");
    }

    @Test
    void snapshot_deleteNonExistentKey_snapshotStillValid() throws IOException {
        KVMap map = new KVMap();
        map.put("existingKey", "value".getBytes(StandardCharsets.UTF_8));

        // Try to delete non-existent key
        boolean removed = map.remove("nonExistentKey");

        assertFalse(removed, "Remove should return false for non-existent key");

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Existing key should still be there
        assertNotNull(restoredMap.get("existingKey"), "existingKey should still exist");
    }

    @Test
    void snapshot_afterMultipleDeletes_snapshotReflectsState() throws IOException {
        KVMap map = new KVMap();
        for (int i = 0; i < 10; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Delete every other key
        for (int i = 0; i < 10; i += 2) {
            map.remove("key" + i);
        }

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Verify odd keys exist, even keys don't
        for (int i = 0; i < 10; i++) {
            KVMap.Node node = restoredMap.get("key" + i);
            if (i % 2 == 0) {
                assertNull(node, "key" + i + " should be deleted");
            } else {
                assertNotNull(node, "key" + i + " should exist");
            }
        }
    }

    @Test
    void snapshot_deleteAndReAddKey_keyPresentInSnapshot() throws IOException {
        KVMap map = new KVMap();
        map.put("reAddKey", "originalValue".getBytes(StandardCharsets.UTF_8));

        // Delete and re-add with new value
        map.remove("reAddKey");
        map.put("reAddKey", "newValue".getBytes(StandardCharsets.UTF_8));

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Key should exist with new value
        KVMap.Node node = restoredMap.get("reAddKey");
        assertNotNull(node, "reAddKey should exist");
        assertArrayEquals("newValue".getBytes(StandardCharsets.UTF_8), node.value());
    }

    @Test
    void snapshot_deleteKeyWithSpecialCharacters_succeeds() throws IOException {
        KVMap map = new KVMap();
        String specialKey = "key-with_special.chars:123!@#$%";
        map.put(specialKey, "value".getBytes(StandardCharsets.UTF_8));

        // Delete key with special characters
        boolean removed = map.remove(specialKey);

        assertTrue(removed, "Delete should succeed for key with special characters");

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);
        assertNull(restoredMap.get(specialKey), "Special key should be deleted");
    }

    @Test
    void snapshot_deleteKeyWithUnicode_succeeds() throws IOException {
        KVMap map = new KVMap();
        String unicodeKey = "键值ストア🔑";
        map.put(unicodeKey, "value".getBytes(StandardCharsets.UTF_8));

        // Delete unicode key
        boolean removed = map.remove(unicodeKey);

        assertTrue(removed, "Delete should succeed for unicode key");

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);
        assertNull(restoredMap.get(unicodeKey), "Unicode key should be deleted");
    }

    // --- DELETE Integration Tests ---

    @Test
    void snapshot_roundTripWithDelete_preservesState() throws IOException {
        KVMap map = new KVMap();
        map.put("keep1", "value1".getBytes(StandardCharsets.UTF_8));
        map.put("delete1", "value2".getBytes(StandardCharsets.UTF_8));
        map.put("keep2", "value3".getBytes(StandardCharsets.UTF_8));

        // Take first snapshot
        Path fpath1 = snapshotter.getDir().resolve("snapshot1");
        snapshotter.snapshot(map, fpath1);

        // Delete a key
        map.remove("delete1");

        // Take second snapshot
        Path fpath2 = snapshotter.getDir().resolve("snapshot2");
        snapshotter.snapshot(map, fpath2);

        // Load first snapshot - should have all keys
        KVMap restored1 = snapshotter.loadSnapshot(fpath1);
        assertNotNull(restored1.get("keep1"), "keep1 should exist in first snapshot");
        assertNotNull(restored1.get("delete1"), "delete1 should exist in first snapshot");
        assertNotNull(restored1.get("keep2"), "keep2 should exist in first snapshot");

        // Load second snapshot - should not have deleted key
        KVMap restored2 = snapshotter.loadSnapshot(fpath2);
        assertNotNull(restored2.get("keep1"), "keep1 should exist in second snapshot");
        assertNull(restored2.get("delete1"), "delete1 should not exist in second snapshot");
        assertNotNull(restored2.get("keep2"), "keep2 should exist in second snapshot");
    }

    @Test
    void snapshot_afterPutDeletePut_cycleWorks() throws IOException {
        KVMap map = new KVMap();

        // Put
        map.put("cycleKey", "firstValue".getBytes(StandardCharsets.UTF_8));

        // Delete
        map.remove("cycleKey");

        // Put again
        map.put("cycleKey", "secondValue".getBytes(StandardCharsets.UTF_8));

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        KVMap.Node node = restoredMap.get("cycleKey");
        assertNotNull(node, "cycleKey should exist after put-delete-put cycle");
        assertArrayEquals("secondValue".getBytes(StandardCharsets.UTF_8), node.value());
    }

    @Test
    void snapshot_deleteOnlyKey_emptySnapshot() throws IOException {
        KVMap map = new KVMap();
        map.put("onlyKey", "onlyValue".getBytes(StandardCharsets.UTF_8));

        // Delete the only key
        map.remove("onlyKey");

        Path fpath = snapshotter.getDir().resolve("snapshot");
        assertDoesNotThrow(() -> snapshotter.snapshot(map, fpath));

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);
        assertNull(restoredMap.get("onlyKey"), "onlyKey should not exist");
    }

    @Test
    void snapshot_deleteFromLargeMap_snapshotCorrect() throws IOException {
        KVMap map = new KVMap();

        // Add many keys
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Delete some keys
        for (int i = 25; i < 75; i++) {
            map.remove("key" + i);
        }

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);

        // Verify keys 0-24 exist
        for (int i = 0; i < 25; i++) {
            assertNotNull(restoredMap.get("key" + i), "key" + i + " should exist");
        }

        // Verify keys 25-74 don't exist
        for (int i = 25; i < 75; i++) {
            assertNull(restoredMap.get("key" + i), "key" + i + " should be deleted");
        }

        // Verify keys 75-99 exist
        for (int i = 75; i < 100; i++) {
            assertNotNull(restoredMap.get("key" + i), "key" + i + " should exist");
        }
    }

    @Test
    void snapshot_deleteOverwrittenKey_snapshotCorrect() throws IOException {
        KVMap map = new KVMap();

        // Put, overwrite, then delete
        map.put("overwrittenKey", "initialValue".getBytes(StandardCharsets.UTF_8));
        map.put("overwrittenKey", "overwrittenValue".getBytes(StandardCharsets.UTF_8));
        map.remove("overwrittenKey");

        Path fpath = snapshotter.getDir().resolve("snapshot");
        snapshotter.snapshot(map, fpath);

        KVMap restoredMap = snapshotter.loadSnapshot(fpath);
        assertNull(restoredMap.get("overwrittenKey"), "overwrittenKey should be deleted");
    }
}
