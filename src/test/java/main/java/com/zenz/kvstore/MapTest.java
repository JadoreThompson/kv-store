package main.java.com.zenz.kvstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class MapTest {

    private Map map;

    @BeforeEach
    void setUp() {
        map = new Map();
    }

    // --- put / get ---

    @Test
    void put_thenGet_returnsNode() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        map.put("key1", value);

        Map.Node node = map.get("key1");

        assertNotNull(node);
        assertArrayEquals(value, node.value);
        assertEquals("key1", node.key);
    }

    @Test
    void get_missingKey_returnsNull() {
        Map.Node node = map.get("nonexistent");
        assertNull(node);
    }

    @Test
    void put_sameKeyTwice_updatesValue() {
        map.put("key1", "first".getBytes(StandardCharsets.UTF_8));
        map.put("key1", "second".getBytes(StandardCharsets.UTF_8));

        Map.Node node = map.get("key1");

        assertNotNull(node);
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value);
    }

    @Test
    void put_multipleKeys_allRetrievable() {
        map.put("a", "alpha".getBytes(StandardCharsets.UTF_8));
        map.put("b", "beta".getBytes(StandardCharsets.UTF_8));
        map.put("c", "gamma".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), map.get("a").value);
        assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), map.get("b").value);
        assertArrayEquals("gamma".getBytes(StandardCharsets.UTF_8), map.get("c").value);
    }

    // --- byte[] values ---

    @Test
    void put_integerAsBytes_retrievesCorrectly() {
        byte[] value = ByteBuffer.allocate(4).putInt(42).array();
        map.put("intKey", value);

        Map.Node node = map.get("intKey");

        assertNotNull(node);
        int restored = ByteBuffer.wrap(node.value).getInt();
        assertEquals(42, restored);
    }

    @Test
    void put_emptyByteArray_retrievesCorrectly() {
        byte[] value = new byte[0];
        map.put("emptyKey", value);

        Map.Node node = map.get("emptyKey");

        assertNotNull(node);
        assertArrayEquals(new byte[0], node.value);
    }

    // --- collision handling ---

    @Test
    void put_collidingKeys_bothRetrievable() {
        // Find two keys that hash to the same bucket to test chaining
        String key1 = "key1";
        String key2 = findCollidingKey(key1, Map.INITIAL_CAPACITY);

        if (key2 != null) {
            map.put(key1, "value1".getBytes(StandardCharsets.UTF_8));
            map.put(key2, "value2".getBytes(StandardCharsets.UTF_8));

            assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), map.get(key1).value);
            assertArrayEquals("value2".getBytes(StandardCharsets.UTF_8), map.get(key2).value);
        }
    }

    /** Finds a key that hashes to the same bucket as {@code target} within {@code capacity}. */
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
}
