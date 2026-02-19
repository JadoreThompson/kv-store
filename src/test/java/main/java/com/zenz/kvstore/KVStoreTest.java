package com.zenz.kvstore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class KVStoreTest {

    private static String WAL_FNAME = "test.log";
    private File file;
    private KVStore store;
    private WALogger logger;

    public KVStoreTest() {
        file = new File(WAL_FNAME);
    }

    @BeforeEach
    void setUp() throws IOException {
        file.createNewFile();
        logger = new WALogger(file.getName());
        store = new KVStore(logger);
    }

    @AfterEach
    void tearDown() throws IOException {
        logger.close();
        file.delete();
    }

    // --- put / get ---

    @Test
    void put_thenGet_returnsNode() throws IOException {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value);

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals(value, node.value);
        assertEquals("key1", node.key);
    }

    @Test
    void get_missingKey_returnsNull() throws IOException {
        KVMap.Node node = store.get("nonexistent");
        assertNull(node);
    }

    @Test
    void put_sameKeyTwice_updatesValue() throws IOException {
        store.put("key1", "first".getBytes(StandardCharsets.UTF_8));
        store.put("key1", "second".getBytes(StandardCharsets.UTF_8));

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value);
    }

    @Test
    void put_multipleKeys_allRetrievable() throws IOException {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.put("age", ByteBuffer.allocate(4).putInt(30).array());
        store.put("city", "london".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8), store.get("name").value);
        assertEquals(30, ByteBuffer.wrap(store.get("age").value).getInt());
        assertArrayEquals("london".getBytes(StandardCharsets.UTF_8), store.get("city").value);
    }

    // --- byte[] values ---

    @Test
    void put_integerAsBytes_retrievesCorrectly() throws IOException {
        byte[] value = ByteBuffer.allocate(4).putInt(99).array();
        store.put("count", value);

        KVMap.Node node = store.get("count");

        assertNotNull(node);
        assertEquals(99, ByteBuffer.wrap(node.value).getInt());
    }

    @Test
    void put_emptyByteArray_retrievesCorrectly() throws IOException {
        store.put("empty", new byte[0]);

        KVMap.Node node = store.get("empty");

        assertNotNull(node);
        assertArrayEquals(new byte[0], node.value);
    }

    // --- large number of entries ---

    @Test
    void put_manyEntries_allRetrievable() throws IOException {
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
    void put_logsOperationToWAL() throws IOException {
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        String walContents = Files.readString(file.toPath());

        assertTrue(walContents.contains("PUT"), "WAL should contain PUT operation");
        assertTrue(walContents.contains("walKey"), "WAL should contain the key");
    }

    @Test
    void get_logsOperationToWAL() throws IOException {
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        store.get("walKey");
        logger.close();

        String walContents = Files.readString(file.toPath());

        assertTrue(walContents.contains("GET"), "WAL should contain GET operation");
        assertTrue(walContents.contains("walKey"), "WAL should contain the key");
    }

    @Test
    void put_andGet_bothLoggedToWAL() throws IOException {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.get("name");
        logger.close();

        String walContents = Files.readString(file.toPath());
        String[] lines = walContents.strip().split("\n");

        assertEquals(2, lines.length, "WAL should have exactly 2 entries");
        assertTrue(lines[0].contains("PUT"), "First entry should be PUT");
        assertTrue(lines[1].contains("GET"), "Second entry should be GET");
    }

    @Test
    void multipleOperations_allLoggedToWAL() throws IOException {
        store.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        store.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        store.get("k1");
        store.get("k2");
        logger.close();

        String walContents = Files.readString(file.toPath());
        String[] lines = walContents.strip().split("\n");

        assertEquals(4, lines.length, "WAL should have exactly 4 entries");
        assertTrue(lines[0].contains("PUT") && lines[0].contains("k1"), "Line 1 should be PUT k1");
        assertTrue(lines[1].contains("PUT") && lines[1].contains("k2"), "Line 2 should be PUT k2");
        assertTrue(lines[2].contains("GET") && lines[2].contains("k1"), "Line 3 should be GET k1");
        assertTrue(lines[3].contains("GET") && lines[3].contains("k2"), "Line 4 should be GET k2");
    }
}
