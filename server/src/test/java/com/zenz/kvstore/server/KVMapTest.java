package com.zenz.kvstore.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class KVMapTest {

    private KVMap map;

    @BeforeEach
    void setUp() {
        map = new KVMap();
    }

    @Test
    void put_thenGet_returnsNode() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        map.put("key1", value);

        KVMap.Node node = map.get("key1");

        assertNotNull(node);
        assertArrayEquals(value, node.value());
        assertEquals("key1", node.key());
    }

    @Test
    void get_missingKey_returnsNull() {
        KVMap.Node node = map.get("nonexistent");
        assertNull(node);
    }

    @Test
    void put_sameKeyTwice_updatesValue() {
        map.put("key1", "first".getBytes(StandardCharsets.UTF_8));
        map.put("key1", "second".getBytes(StandardCharsets.UTF_8));

        KVMap.Node node = map.get("key1");

        assertNotNull(node);
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value());
    }

    @Test
    void put_multipleKeys_allRetrievable() {
        map.put("a", "alpha".getBytes(StandardCharsets.UTF_8));
        map.put("b", "beta".getBytes(StandardCharsets.UTF_8));
        map.put("c", "gamma".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), map.get("a").value());
        assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), map.get("b").value());
        assertArrayEquals("gamma".getBytes(StandardCharsets.UTF_8), map.get("c").value());
    }

    @Test
    void put_integerAsBytes_retrievesCorrectly() {
        byte[] value = ByteBuffer.allocate(4).putInt(42).array();
        map.put("intKey", value);

        KVMap.Node node = map.get("intKey");

        assertNotNull(node);
        int restored = ByteBuffer.wrap(node.value()).getInt();
        assertEquals(42, restored);
    }

    @Test
    void put_emptyByteArray_retrievesCorrectly() {
        byte[] value = new byte[0];
        map.put("emptyKey", value);

        KVMap.Node node = map.get("emptyKey");

        assertNotNull(node);
        assertArrayEquals(new byte[0], node.value());
    }

    @Test
    void put_collidingKeys_bothRetrievable() {
        // Find two keys that hash to the same bucket to test chaining
        String key1 = "key1";
        String key2 = findCollidingKey(key1, map.capacity());

        if (key2 != null) {
            map.put(key1, "value1".getBytes(StandardCharsets.UTF_8));
            map.put(key2, "value2".getBytes(StandardCharsets.UTF_8));

            assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), map.get(key1).value());
            assertArrayEquals("value2".getBytes(StandardCharsets.UTF_8), map.get(key2).value());
        }
    }

    // === Remove Unit Tests ===

    @Test
    void remove_existingKey_returnsTrue() {
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        boolean result = map.remove("key1");

        assertTrue(result);
    }

    @Test
    void remove_missingKey_returnsFalse() {
        boolean result = map.remove("nonexistent");

        assertFalse(result);
    }

    @Test
    void remove_existingKey_keyNoLongerRetrievable() {
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        map.remove("key1");
        KVMap.Node node = map.get("key1");

        assertNull(node);
    }

    @Test
    void remove_sameKeyTwice_secondReturnsFalse() {
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        boolean firstRemove = map.remove("key1");
        boolean secondRemove = map.remove("key1");

        assertTrue(firstRemove);
        assertFalse(secondRemove);
    }

    @Test
    void remove_oneOfMultipleKeys_onlyRemovedKeyIsGone() {
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        map.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        map.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        assertTrue(map.getHt1().size() == 3, "Hash table 1 should contain 3 nodes");
        map.remove("key2");

        assertTrue(map.getHt1().size() == 2, "Hash table 1 should contain 2 nodes");
        assertNotNull(map.get("key1"));
        assertNull(map.get("key2"));
        assertNotNull(map.get("key3"));
    }

    @Test
    void remove_multipleKeysAllRemoved_allReturnNull() {
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        map.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        map.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        map.remove("key1");
        map.remove("key2");
        map.remove("key3");

        assertNull(map.get("key1"));
        assertNull(map.get("key2"));
        assertNull(map.get("key3"));
    }

    @Test
    void remove_collidingKeys_bothCanBeRemoved() {
        String key1 = "key1";
        String key2 = findCollidingKey(key1, map.capacity());

        if (key2 != null) {
            map.put(key1, "value1".getBytes(StandardCharsets.UTF_8));
            map.put(key2, "value2".getBytes(StandardCharsets.UTF_8));

            boolean remove1 = map.remove(key1);
            boolean remove2 = map.remove(key2);

            assertTrue(remove1);
            assertTrue(remove2);
            assertNull(map.get(key1));
            assertNull(map.get(key2));
        }
    }

    @Test
    void remove_collidingKeys_removeOneOtherRemains() {
        String key1 = "key1";
        String key2 = findCollidingKey(key1, map.capacity());

        if (key2 != null) {
            map.put(key1, "value1".getBytes(StandardCharsets.UTF_8));
            map.put(key2, "value2".getBytes(StandardCharsets.UTF_8));

            map.remove(key1);

            assertNull(map.get(key1));
            assertNotNull(map.get(key2));
            assertArrayEquals("value2".getBytes(StandardCharsets.UTF_8), map.get(key2).value());
        }
    }

    @Test
    void remove_fromEmptyMap_returnsFalse() {
        boolean result = map.remove("anyKey");

        assertFalse(result);
    }

    @Test
    void remove_afterUpdate_stillRemovesSuccessfully() {
        map.put("key1", "original".getBytes(StandardCharsets.UTF_8));
        map.put("key1", "updated".getBytes(StandardCharsets.UTF_8));

        boolean result = map.remove("key1");

        assertTrue(result);
        assertNull(map.get("key1"));
    }

    @Test
    void remove_emptyByteArrayValue_removesSuccessfully() {
        map.put("key1", new byte[0]);

        boolean result = map.remove("key1");

        assertTrue(result);
        assertNull(map.get("key1"));
    }

    @Test
    void remove_nullKey_throwsNullPointerException() {
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        assertThrows(NullPointerException.class, () -> map.remove(null));
    }

    // === Remove Integration Tests ===

    @Test
    void remove_duringRehash_keyRemovedFromCorrectTable() throws Exception {
        // Force rehashing by adding many entries
        map = new KVMap(0.7f, KVMap.REHASH_BUCKETS);
        int threshold = (int) (map.capacity() * map.loadFactor());
        for (int i = 0; i < threshold; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Verify we're in rehashing state
        assertTrue(map.isRehashing(), "Map should be in rehashing state");

        // Remove a key during rehashing
        boolean result = map.remove("key5");

        assertTrue(result);
        assertNull(map.get("key5"));
    }

    @Test
    void remove_duringRehash_multipleRemovesSucceed() throws Exception {
        // Force rehashing
        map = new KVMap(0.5f, KVMap.REHASH_BUCKETS);
        int threshold = (int) (map.capacity() * map.loadFactor());
        for (int i = 0; i < threshold; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        assertTrue(map.isRehashing(), "Map should be in rehashing state");

        // Remove multiple keys during rehashing
        assertTrue(map.remove("key0"));
        assertTrue(map.remove("key1"));
        assertTrue(map.remove("key2"));

        assertNull(map.get("key0"));
        assertNull(map.get("key1"));
        assertNull(map.get("key2"));
    }

    @Test
    void remove_afterRehashCompletes_keySuccessfullyRemoved() throws Exception {
        // Force rehashing and complete it
        int threshold = (int) (map.capacity() * map.loadFactor());
        for (int i = 0; i < threshold + 10; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Complete rehashing by calling rehash until done
        while (map.isRehashing()) {
            map.rehash();
        }

        assertFalse(map.isRehashing(), "Map should not be in rehashing state");

        // Remove a key after rehashing completes
        boolean result = map.remove("key5");

        assertTrue(result);
        assertNull(map.get("key5"));
    }

    @Test
    void remove_largeNumberOfKeys_allRemovedSuccessfully() {
        int count = 100;
        for (int i = 0; i < count; i++) {
            map.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Remove all keys
        for (int i = 0; i < count; i++) {
            boolean result = map.remove("key" + i);
            assertTrue(result, "Remove should succeed for key" + i);
        }

        // Verify all are removed
        for (int i = 0; i < count; i++) {
            assertNull(map.get("key" + i), "Key " + i + " should be removed");
        }
    }

    @Test
    void remove_interleavedWithPut_operationsConsistent() {
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        map.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        map.remove("key1");
        map.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        map.remove("key2");
        map.put("key1", "newValue1".getBytes(StandardCharsets.UTF_8));

        assertNull(map.get("key2"));
        assertNotNull(map.get("key1"));
        assertNotNull(map.get("key3"));
        assertArrayEquals("newValue1".getBytes(StandardCharsets.UTF_8), map.get("key1").value());
    }

    @Test
    void remove_onlyKeyInBucket_bucketIsEmpty() {
        // Add a single key
        map.put("uniqueKey", "value".getBytes(StandardCharsets.UTF_8));

        // Remove it
        boolean result = map.remove("uniqueKey");

        assertTrue(result);
        assertNull(map.get("uniqueKey"));

        // Add same key again to verify bucket is still functional
        map.put("uniqueKey", "newValue".getBytes(StandardCharsets.UTF_8));
        assertNotNull(map.get("uniqueKey"));
    }

    @Test
    void remove_headOfNodeList_success() {
        // Find colliding keys to create a chain
        String key1 = "key1";
        String key2 = findCollidingKey(key1, map.capacity());

        if (key2 != null) {
            map.put(key1, "value1".getBytes(StandardCharsets.UTF_8));
            map.put(key2, "value2".getBytes(StandardCharsets.UTF_8));

            // Remove the first key (head of chain)
            boolean result = map.remove(key1);

            assertTrue(result);
            assertNull(map.get(key1));
            assertNotNull(map.get(key2));
        }
    }

    @Test
    void remove_tailOfNodeList_success() {
        // Find colliding keys to create a chain
        String key1 = "key1";
        String key2 = findCollidingKey(key1, map.capacity());

        if (key2 != null) {
            map.put(key1, "value1".getBytes(StandardCharsets.UTF_8));
            map.put(key2, "value2".getBytes(StandardCharsets.UTF_8));

            // Remove the second key (tail of chain)
            boolean result = map.remove(key2);

            assertTrue(result);
            assertNull(map.get(key2));
            assertNotNull(map.get(key1));
        }
    }

    @Test
    void remove_middleOfNodeList_success() {
        // Find three colliding keys to create a chain
        String key1 = "key1";
        String key2 = findCollidingKey(key1, map.capacity());
        String key3 = findCollidingKey(key1, map.capacity());

        if (key2 != null && key3 != null && !key2.equals(key3)) {
            map.put(key1, "value1".getBytes(StandardCharsets.UTF_8));
            map.put(key2, "value2".getBytes(StandardCharsets.UTF_8));
            map.put(key3, "value3".getBytes(StandardCharsets.UTF_8));

            // Remove the middle key
            boolean result = map.remove(key2);

            assertTrue(result);
            assertNull(map.get(key2));
            assertNotNull(map.get(key1));
            assertNotNull(map.get(key3));
        }
    }

    @Test
    void remove_keyWithSpecialCharacters_removesSuccessfully() {
        String specialKey = "key-with-特殊字符-émoji-🎉";
        map.put(specialKey, "value".getBytes(StandardCharsets.UTF_8));

        boolean result = map.remove(specialKey);

        assertTrue(result);
        assertNull(map.get(specialKey));
    }

    @Test
    void remove_keyWithWhitespace_removesSuccessfully() {
        String whitespaceKey = "key with spaces\tand\ttabs";
        map.put(whitespaceKey, "value".getBytes(StandardCharsets.UTF_8));

        boolean result = map.remove(whitespaceKey);

        assertTrue(result);
        assertNull(map.get(whitespaceKey));
    }

    @Test
    void remove_largeValue_removesSuccessfully() {
        byte[] largeValue = new byte[10000];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }
        map.put("largeKey", largeValue);

        boolean result = map.remove("largeKey");

        assertTrue(result);
        assertNull(map.get("largeKey"));
    }

    /**
     * Finds a key that hashes to the same bucket as {@code target} within {@code capacity}.
     */
    private String findCollidingKey(String target, int capacity) {
        int targetBucket = Math.abs(target.hashCode()) % capacity;
        for (int i = 0; i < 100_000; i++) {
            String candidate = "collision_" + i;
            if (Math.abs(candidate.hashCode()) % capacity == targetBucket && !candidate.equals(target)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Checks if the map is currently rehashing using reflection.
     */
    private boolean isRehashing(KVMap map) throws Exception {
        Field rehashIdxField = KVMap.class.getDeclaredField("rehashIdx");
        rehashIdxField.setAccessible(true);
        int rehashIdx = (int) rehashIdxField.get(map);
        return rehashIdx >= 0;
    }
}
