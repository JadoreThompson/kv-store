package com.zenz.kvstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class KVStoreTest {

    private KVStore store;

    @BeforeEach
    void setUp() {
        store = new KVStore();
    }

    // --- put / get ---

    @Test
    void put_thenGet_returnsNode() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value);

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals(value, node.value);
        assertEquals("key1", node.key);
    }

    @Test
    void get_missingKey_returnsNull() {
        KVMap.Node node = store.get("nonexistent");
        assertNull(node);
    }

    @Test
    void put_sameKeyTwice_updatesValue() {
        store.put("key1", "first".getBytes(StandardCharsets.UTF_8));
        store.put("key1", "second".getBytes(StandardCharsets.UTF_8));

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value);
    }

    @Test
    void put_multipleKeys_allRetrievable() {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.put("age", ByteBuffer.allocate(4).putInt(30).array());
        store.put("city", "london".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8), store.get("name").value);
        assertEquals(30, ByteBuffer.wrap(store.get("age").value).getInt());
        assertArrayEquals("london".getBytes(StandardCharsets.UTF_8), store.get("city").value);
    }

    // --- byte[] values ---

    @Test
    void put_integerAsBytes_retrievesCorrectly() {
        byte[] value = ByteBuffer.allocate(4).putInt(99).array();
        store.put("count", value);

        KVMap.Node node = store.get("count");

        assertNotNull(node);
        assertEquals(99, ByteBuffer.wrap(node.value).getInt());
    }

    @Test
    void put_emptyByteArray_retrievesCorrectly() {
        store.put("empty", new byte[0]);

        KVMap.Node node = store.get("empty");

        assertNotNull(node);
        assertArrayEquals(new byte[0], node.value);
    }

    // --- large number of entries ---

    @Test
    void put_manyEntries_allRetrievable() {
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
}
