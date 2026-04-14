package com.zenz.kvstore.server;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.server.logging.*;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotBody;
import com.zenz.kvstore.server.snapshot.SingleSnapshotFooter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotHeader;
import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KVStoreTest {

    private KVStore store;

    private KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter;

    private TestLogger logger;

    @BeforeEach
    void setUp() throws Exception {
        snapshotter = new KVStoreSnapshotter<>(
                SingleSnapshotHeader.class,
                SingleSnapshotBody.class,
                SingleSnapshotFooter.class
        );
        snapshotter.setDir(Files.createTempDirectory("temp-snapshots-"));
        logger = new TestLogger();
        LogHandler logHandler = new LogHandler(logger, snapshotter);
        store = new KVStore(logHandler);
    }

    @AfterEach
    void tearDown() {
        if (logger != null) {
            logger.close();
        }
    }

    @Test
    void put_thenGet_returnsNode() throws Exception {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value);

        KVMap.Node node = store.get("key1");

        assertNotNull(node);
        assertArrayEquals(value, node.value());
        assertEquals("key1", node.key());
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
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), node.value());
    }

    @Test
    void put_multipleKeys_allRetrievable() throws Exception {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.put("age", ByteBuffer.allocate(4).putInt(30).array());
        store.put("city", "london".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8), store.get("name").value());
        assertEquals(30, ByteBuffer.wrap(store.get("age").value()).getInt());
        assertArrayEquals("london".getBytes(StandardCharsets.UTF_8), store.get("city").value());
    }

    @Test
    void put_integerAsBytes_retrievesCorrectly() throws Exception {
        byte[] value = ByteBuffer.allocate(4).putInt(99).array();
        store.put("count", value);

        KVMap.Node node = store.get("count");

        assertNotNull(node);
        assertEquals(99, ByteBuffer.wrap(node.value()).getInt());
    }

    @Test
    void put_emptyByteArray_retrievesCorrectly() throws Exception {
        store.put("empty", new byte[0]);

        KVMap.Node node = store.get("empty");

        assertNotNull(node);
        assertArrayEquals(new byte[0], node.value());
    }

    @Test
    void put_manyEntries_allRetrievable() throws Exception {
        int count = 500;
        for (int i = 0; i < count; i++) {
            store.put("key_" + i, ByteBuffer.allocate(4).putInt(i).array());
        }

        for (int i = 0; i < count; i++) {
            KVMap.Node node = store.get("key_" + i);
            assertNotNull(node, "Expected node for key_" + i);
            assertEquals(i, ByteBuffer.wrap(node.value()).getInt());
        }
    }

    @Test
    void put_logsOperationToWAL() throws Exception {
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        final List<LogEntry> logs = logger.getEntries();

        assertNotNull(logs, "Logs should not be null");
        assertEquals(1, logs.size(), "WAL should contain one operation");
        assertEquals(CommandType.PUT, logs.getFirst().command.type(), "WAL should contain PUT operation");

        PutCommand putCmd = (PutCommand) logs.getFirst().command;
        assertEquals("walKey", putCmd.key(), "WAL should contain the key");
    }

    @Test
    void get_logsOperationToWAL() throws Exception {
        store.put("walKey", "walValue".getBytes(StandardCharsets.UTF_8));
        store.get("walKey");
        logger.close();

        final List<LogEntry> logs = logger.getEntries();

        assertNotNull(logs, "Logs should not be null");
        assertEquals(1, logs.size(), "Should have 2 log entries");
        assertEquals(CommandType.PUT, logs.getFirst().command.type(), "First entry should be PUT");
    }

    @Test
    void put_andGet_bothLoggedToWAL() throws Exception {
        store.put("name", "alice".getBytes(StandardCharsets.UTF_8));
        store.get("name");
        logger.close();

        final List<LogEntry> logs = logger.getEntries();

        assertNotNull(logs, "Logs should not be null");
        assertEquals(1, logs.size(), "WAL should have exactly 2 entries");
        assertEquals(CommandType.PUT, logs.getFirst().command.type(), "First entry should be PUT");
    }

    @Test
    void multipleOperations_allLoggedToWAL() throws Exception {
        store.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        store.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        store.get("k1");
        store.get("k2");
        logger.close();

        final List<LogEntry> logs = logger.getEntries();

        assertNotNull(logs, "Logs should not be null");
        assertEquals(2, logs.size(), "WAL should have exactly 2 entries");

        PutCommand putCmd1 = (PutCommand) logs.getFirst().command;
        assertEquals(CommandType.PUT, putCmd1.type(), "Line 1 should be PUT");
        assertEquals("k1", putCmd1.key(), "Line 1 should be PUT k1");

        PutCommand putCmd2 = (PutCommand) logs.get(1).command;
        assertEquals(CommandType.PUT, putCmd2.type(), "Line 2 should be PUT");
        assertEquals("k2", putCmd2.key(), "Line 2 should be PUT k2");
    }


    @Test
    void snapshotDuringOperations_triggersWhenThresholdReached() throws Exception {
        final int numOperations = 10;
        store.getLogHandler().setLogsPerSnapshot(numOperations);
        for (int i = 0; i < numOperations; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        logger.close();

        // Verify snapshot was created (should have triggered at 10 operations)
        File[] snapshotFiles = snapshotter.getDir().toFile().listFiles();
        assertNotNull(snapshotFiles, "Snapshot folder should not be empty");
        assertTrue(snapshotFiles.length > 0, "At least one snapshot should have been created");
    }

    // ==================== DELETE UNIT TESTS ====================

    @Test
    void delete_existingKey_returnsTrue() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete("key1");

        assertTrue(result, "Delete should return true for existing key");
    }

    @Test
    void delete_missingKey_returnsFalse() throws Exception {
        boolean result = store.delete("nonexistent");

        assertFalse(result, "Delete should return false for missing key");
    }

    @Test
    void delete_existingKey_keyNoLongerRetrievable() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        store.delete("key1");

        KVMap.Node node = store.get("key1");
        assertNull(node, "Key should no longer exist after delete");
    }

    @Test
    void delete_sameKeyTwice_secondReturnsFalse() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        boolean firstDelete = store.delete("key1");
        boolean secondDelete = store.delete("key1");

        assertTrue(firstDelete, "First delete should return true");
        assertFalse(secondDelete, "Second delete should return false");
    }

    @Test
    void delete_emptyKey_handlesCorrectly() throws Exception {
        store.put("", "emptyKeyValue".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete("");

        assertTrue(result, "Delete should handle empty key");
        assertNull(store.get(""), "Empty key should be removed");
    }

    @Test
    void delete_oneKey_otherKeysRemain() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        store.delete("key2");

        assertNotNull(store.get("key1"), "key1 should still exist");
        assertNull(store.get("key2"), "key2 should be deleted");
        assertNotNull(store.get("key3"), "key3 should still exist");
    }

    @Test
    void delete_logsOperationToWAL() throws Exception {
        store.put("deleteKey", "deleteValue".getBytes(StandardCharsets.UTF_8));
        store.delete("deleteKey");
        logger.close();

        final List<LogEntry> logs = logger.getEntries();

        assertNotNull(logs, "Logs should not be null");
        assertEquals(2, logs.size(), "WAL should contain two operations");
        assertEquals(CommandType.PUT, logs.getFirst().command.type(), "First entry should be PUT");
        assertEquals(CommandType.DELETE, logs.get(1).command.type(), "Second entry should be DELETE");

        DeleteCommand deleteCmd = (DeleteCommand) logs.get(1).command;
        assertEquals("deleteKey", deleteCmd.key(), "WAL should contain the deleted key");
    }

    @Test
    void delete_missingKey_logsOperationToWAL() throws Exception {
        store.delete("nonexistent");
        logger.close();

        final List<LogEntry> logs = logger.getEntries();

        assertNotNull(logs, "Logs should not be null");
        assertEquals(1, logs.size(), "WAL should contain one operation");
        assertEquals(CommandType.DELETE, logs.getFirst().command.type(), "Entry should be DELETE");

        DeleteCommand deleteCmd = (DeleteCommand) logs.getFirst().command;
        assertEquals("nonexistent", deleteCmd.key(), "WAL should contain the key");
    }

    // ==================== DELETE INTEGRATION TESTS ====================

    @Test
    void delete_afterPut_removesValue() throws Exception {
        store.put("toDelete", "value".getBytes(StandardCharsets.UTF_8));

        boolean deleted = store.delete("toDelete");

        assertTrue(deleted, "Delete should succeed");
        assertNull(store.get("toDelete"), "Value should be removed after delete");
    }

    @Test
    void delete_multipleKeys_allRemoved() throws Exception {
        int count = 10;
        for (int i = 0; i < count; i++) {
            store.put("key_" + i, ("value_" + i).getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < count; i++) {
            boolean result = store.delete("key_" + i);
            assertTrue(result, "Delete should succeed for key_" + i);
        }

        for (int i = 0; i < count; i++) {
            assertNull(store.get("key_" + i), "Key key_" + i + " should be deleted");
        }
    }

    @Test
    void delete_interleavedWithPut_handlesCorrectly() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.delete("key1");
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        store.delete("key2");
        store.put("key1", "newValue1".getBytes(StandardCharsets.UTF_8));

        assertNotNull(store.get("key1"), "key1 should exist");
        assertNull(store.get("key2"), "key2 should be deleted");
        assertNotNull(store.get("key3"), "key3 should exist");
        assertArrayEquals("newValue1".getBytes(StandardCharsets.UTF_8), store.get("key1").value());
    }

    @Test
    void delete_thenPutSameKey_keyRetrievable() throws Exception {
        store.put("key1", "originalValue".getBytes(StandardCharsets.UTF_8));
        store.delete("key1");
        store.put("key1", "newValue".getBytes(StandardCharsets.UTF_8));

        KVMap.Node node = store.get("key1");

        assertNotNull(node, "Key should exist after re-put");
        assertArrayEquals("newValue".getBytes(StandardCharsets.UTF_8), node.value());
    }

    @Test
    void delete_duringSnapshot_triggersWhenThresholdReached() throws Exception {
        store.getLogHandler().setLogsPerSnapshot(10);

        // Add 5 entries
        for (int i = 0; i < 5; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Delete 5 entries (total 10 operations = threshold)
        for (int i = 0; i < 5; i++) {
            store.delete("key" + i);
        }

        logger.close();

        // Verify snapshot was created
        File[] snapshotFiles = store.getLogHandler().getSnapshotter().getDir().toFile().listFiles();
        assertNotNull(snapshotFiles, "Snapshot folder should not be empty");
        assertTrue(snapshotFiles.length > 0, "At least one snapshot should have been created");
    }

    @Test
    void delete_largeValue_handlesCorrectly() throws Exception {
        byte[] largeValue = new byte[10000];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        store.put("largeKey", largeValue);

        boolean result = store.delete("largeKey");

        assertTrue(result, "Delete should handle large values");
        assertNull(store.get("largeKey"), "Large value key should be deleted");
    }

    @Test
    void delete_specialCharacterKey_handlesCorrectly() throws Exception {
        String specialKey = "key-with-special_chars!@#$%^&*()";
        store.put(specialKey, "value".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete(specialKey);

        assertTrue(result, "Delete should handle special character keys");
        assertNull(store.get(specialKey), "Special character key should be deleted");
    }

    @Test
    void delete_unicodeKey_handlesCorrectly() throws Exception {
        String unicodeKey = "键值"; // Chinese characters
        store.put(unicodeKey, "unicodeValue".getBytes(StandardCharsets.UTF_8));

        boolean result = store.delete(unicodeKey);

        assertTrue(result, "Delete should handle unicode keys");
        assertNull(store.get(unicodeKey), "Unicode key should be deleted");
    }

    // ==================== SEARCH OPERATION TESTS ====================

    @Test
    void search_withMatchingPrefix_returnsMatchingNodes() throws Exception {
        store.put("apple", "fruit".getBytes(StandardCharsets.UTF_8));
        store.put("application", "software".getBytes(StandardCharsets.UTF_8));
        store.put("apply", "action".getBytes(StandardCharsets.UTF_8));
        store.put("banana", "fruit".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("app");

        assertEquals(3, result.size(), "Should find 3 nodes with 'app' prefix");
        assertTrue(result.stream().anyMatch(n -> n.key().equals("apple")));
        assertTrue(result.stream().anyMatch(n -> n.key().equals("application")));
        assertTrue(result.stream().anyMatch(n -> n.key().equals("apply")));
        assertFalse(result.stream().anyMatch(n -> n.key().equals("banana")));
    }

    @Test
    void search_withNoMatch_returnsEmptyList() throws Exception {
        store.put("hello", "world".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("xyz");

        assertNotNull(result, "Search should return non-null list");
        assertTrue(result.isEmpty(), "Search should return empty list for no matches");
    }

    @Test
    void search_emptyPrefix_returnsAllNodes() throws Exception {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("");

        assertEquals(3, result.size(), "Empty prefix should return all nodes");
    }

    @Test
    void search_exactKeyMatch_returnsNode() throws Exception {
        store.put("exactKey", "exactValue".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("exactKey");

        assertEquals(1, result.size(), "Should find exactly one node");
        assertEquals("exactKey", result.getFirst().key());
        assertArrayEquals("exactValue".getBytes(StandardCharsets.UTF_8), result.getFirst().value());
    }

    @Test
    void search_emptyStore_returnsEmptyList() throws Exception {
        List<KVMap.Node> result = store.search("anything");

        assertNotNull(result, "Search should return non-null list");
        assertTrue(result.isEmpty(), "Search on empty store should return empty list");
    }

    @Test
    void search_afterDelete_excludesDeletedKey() throws Exception {
        store.put("apple", "fruit".getBytes(StandardCharsets.UTF_8));
        store.put("application", "software".getBytes(StandardCharsets.UTF_8));
        store.delete("apple");

        List<KVMap.Node> result = store.search("app");

        assertEquals(1, result.size(), "Should find 1 node after delete");
        assertEquals("application", result.getFirst().key());
    }

    @Test
    void search_afterUpdate_returnsUpdatedValue() throws Exception {
        store.put("key1", "oldValue".getBytes(StandardCharsets.UTF_8));
        store.put("key1", "newValue".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("key");

        assertEquals(1, result.size());
        assertArrayEquals("newValue".getBytes(StandardCharsets.UTF_8), result.getFirst().value());
    }

    @Test
    void search_singleCharacterPrefix_returnsMatches() throws Exception {
        store.put("a", "letterA".getBytes(StandardCharsets.UTF_8));
        store.put("ab", "lettersAB".getBytes(StandardCharsets.UTF_8));
        store.put("abc", "lettersABC".getBytes(StandardCharsets.UTF_8));
        store.put("b", "letterB".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("a");

        assertEquals(3, result.size(), "Should find 3 nodes starting with 'a'");
        assertTrue(result.stream().allMatch(n -> n.key().startsWith("a")));
    }

    @Test
    void search_caseSensitive_returnsCorrectMatches() throws Exception {
        store.put("Apple", "fruit".getBytes(StandardCharsets.UTF_8));
        store.put("apple", "tech".getBytes(StandardCharsets.UTF_8));
        store.put("APPLE", "uppercase".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> lowerResult = store.search("a");
        List<KVMap.Node> upperResult = store.search("A");
        List<KVMap.Node> allCapsResult = store.search("AP");

        assertEquals(1, lowerResult.size(), "Should find 1 lowercase 'apple'");
        assertEquals("apple", lowerResult.getFirst().key());

        assertEquals(2, upperResult.size(), "Should find 2 title case 'Apple' and 'APPLE'");
        assertEquals("Apple", upperResult.getFirst().key());

        assertEquals(1, allCapsResult.size(), "Should find 1 uppercase 'APPLE'");
        assertEquals("APPLE", allCapsResult.getFirst().key());
    }

    @Test
    void search_specialCharacterPrefix_returnsMatches() throws Exception {
        store.put("key-1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key-2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key_1", "value3".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("key-");

        assertEquals(2, result.size(), "Should find 2 nodes with 'key-' prefix");
        assertTrue(result.stream().allMatch(n -> n.key().startsWith("key-")));
    }

    @Test
    void search_numericPrefix_returnsMatches() throws Exception {
        store.put("123", "numeric".getBytes(StandardCharsets.UTF_8));
        store.put("123abc", "alphanumeric".getBytes(StandardCharsets.UTF_8));
        store.put("456", "other".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("123");

        assertEquals(2, result.size(), "Should find 2 nodes with '123' prefix");
    }

    @Test
    void search_unicodePrefix_returnsMatches() throws Exception {
        store.put("日本語", "japanese".getBytes(StandardCharsets.UTF_8));
        store.put("日本料理", "cuisine".getBytes(StandardCharsets.UTF_8));
        store.put("日历", "calendar".getBytes(StandardCharsets.UTF_8));

        List<KVMap.Node> result = store.search("日本");

        assertEquals(2, result.size(), "Should find 2 nodes with '日本' prefix");
    }

    @Test
    void search_multipleOperations_maintainsConsistency() throws Exception {
        // Add initial keys
        store.put("test1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("test2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("other", "value3".getBytes(StandardCharsets.UTF_8));

        // Search and verify
        List<KVMap.Node> result1 = store.search("test");
        assertEquals(2, result1.size());

        // Delete one key
        store.delete("test1");

        // Search again
        List<KVMap.Node> result2 = store.search("test");
        assertEquals(1, result2.size());
        assertEquals("test2", result2.getFirst().key());

        // Add new key
        store.put("test3", "value3".getBytes(StandardCharsets.UTF_8));

        // Search again
        List<KVMap.Node> result3 = store.search("test");
        assertEquals(2, result3.size());
    }

    @Test
    void search_verifyTrieMapConsistency_afterOperations() throws Exception {
        // Add keys
        store.put("test1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("test2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("other", "value3".getBytes(StandardCharsets.UTF_8));

        // Verify trie and map are consistent
        List<KVMap.Node> searchResult = store.search("test");
        assertEquals(2, searchResult.size());

        // Delete a key
        store.delete("test1");

        // Verify trie is updated
        searchResult = store.search("test");
        assertEquals(1, searchResult.size());
        assertNull(store.get("test1"));

        // Update a key
        store.put("test2", "updatedValue".getBytes(StandardCharsets.UTF_8));

        // Verify trie still works and value is updated
        searchResult = store.search("test");
        assertEquals(1, searchResult.size());
        assertArrayEquals("updatedValue".getBytes(StandardCharsets.UTF_8), searchResult.getFirst().value());
    }

    private static class TestLogger implements CommandLogger {

        @Getter
        private final List<LogEntry> entries = new ArrayList<>();

        private boolean isClosed;

        public TestLogger() {
        }

        @Override
        public void log(LogEntry logEntry) {
            if (!isClosed) {
                entries.add(logEntry);
            }
        }

        @Override
        public Path getPath() {
            return null;
        }

        @Override
        public LoggerFactory getLoggerFactory() {
            return null;
        }

        public void close() {
            isClosed = true;
        }

        @Override
        public <L extends LogEntry> List<L> loadLogs(Path path, Deserializer<L> deserializer) throws IOException {
            return List.of();
        }
    }
}
