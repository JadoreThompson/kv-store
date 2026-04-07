package com.zenz.kvstore.server;

import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.commands.GetCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.enums.ResponseType;
import com.zenz.kvstore.common.response.BaseResponse;
import com.zenz.kvstore.common.response.ErrorResponse;
import com.zenz.kvstore.common.response.GetResponse;
import com.zenz.kvstore.common.response.PutResponse;
import com.zenz.kvstore.server.command.handler.CommandHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handler.LogHandler;
import com.zenz.kvstore.server.restorer.Restorer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KVServerTest {

    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 9999;

    private static ExecutorService serverExecutor;
    private static KVServer server;
    private static Path tempDir;
    private static Path logsFolderPath;
    private static Path snapshotFolderPath;
    private static KVMapSnapshotter snapshotter;
    private static WALogger logger;

    @BeforeAll
    static void startServer() throws Exception {
        tempDir = Files.createTempDirectory("kvstore-test-");
        logsFolderPath = tempDir.resolve("logs");
        snapshotFolderPath = tempDir.resolve("snapshots");

        logsFolderPath.toFile().mkdir();
        snapshotFolderPath.toFile().mkdir();

        snapshotter = new KVMapSnapshotter(snapshotFolderPath);
        Path path = logsFolderPath.resolve("app.log");
        if (!Files.exists(path)) Files.createFile(path);
        logger = new WALogger(path);
        KVStore.Builder builder = new KVStore.Builder()
                .setLogHandler(new LogHandler(logger))
                .setSnapshotter(snapshotter)
                .setSnapshotEnabled(false);
        KVStore store = new Restorer().restore(builder);
        server = new KVServer(TEST_HOST, TEST_PORT, new CommandHandler(store));

        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            try {
                server.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Wait for server to start
        Thread.sleep(500);
    }

    @AfterAll
    static void stopServer() throws IOException {
        server.stop();
        serverExecutor.shutdown();
        try {
            serverExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (logger != null) {
            logger.close();
        }
        tempDir.toFile().delete();
    }

    private SocketChannel connectClient() throws IOException {
        SocketChannel client = SocketChannel.open();
        client.configureBlocking(true);
        client.connect(new InetSocketAddress(TEST_HOST, TEST_PORT));
        return client;
    }

    private void sendCommand(SocketChannel client, Command command) throws IOException {
        byte[] serialized = command.serialize();
        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        while (buffer.hasRemaining()) {
            client.write(buffer);
        }
    }

    private BaseResponse receiveResponse(SocketChannel client) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int bytesRead = client.read(buffer);
        if (bytesRead <= 0) {
            return null;
        }
        buffer.flip();
        return BaseResponse.deserialize(buffer);
    }

    @Test
    void clientCanConnect() throws IOException {
        SocketChannel client = connectClient();
        assertTrue(client.isConnected(), "Client should be connected");
        client.close();
    }

    @Test
    void multipleClientsCanConnect() throws IOException {
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        assertTrue(client1.isConnected());
        assertTrue(client2.isConnected());
        assertTrue(client3.isConnected());

        client1.close();
        client2.close();
        client3.close();
    }

    @Test
    void put_returnsOk() throws IOException {
        SocketChannel client = connectClient();
        sendCommand(client, new PutCommand("testkey", "testvalue".getBytes(StandardCharsets.UTF_8)));
        BaseResponse response = receiveResponse(client);

        assertInstanceOf(PutResponse.class, response, "Response should be PutResponse");
        assertEquals(ResponseType.PUT_RESPONSE, response.type());
        client.close();
    }

    @Test
    void put_storesInMap() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("mykey", "myvalue".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);  // consume PutResponse

        // Verify via GET
        sendCommand(client, new GetCommand("mykey"));
        BaseResponse response = receiveResponse(client);

        assertInstanceOf(GetResponse.class, response, "Response should be GetResponse");
        GetResponse getResponse = (GetResponse) response;
        assertEquals("myvalue", new String(getResponse.value(), StandardCharsets.UTF_8));
        client.close();
    }

    @Test
    void put_withSpacesInValue() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("greeting", "hello world".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);  // consume PutResponse

        sendCommand(client, new GetCommand("greeting"));
        BaseResponse response = receiveResponse(client);

        assertInstanceOf(GetResponse.class, response, "Response should be GetResponse");
        GetResponse getResponse = (GetResponse) response;
        assertEquals("hello world", new String(getResponse.value(), StandardCharsets.UTF_8));
        client.close();
    }

    // --- GET Tests ---

    @Test
    void get_existingKey_returnsValue() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("existingkey", "existingvalue".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);  // consume PutResponse

        sendCommand(client, new GetCommand("existingkey"));
        BaseResponse response = receiveResponse(client);

        assertInstanceOf(GetResponse.class, response, "Response should be GetResponse");
        GetResponse getResponse = (GetResponse) response;
        assertEquals("existingvalue", new String(getResponse.value(), StandardCharsets.UTF_8));
        client.close();
    }

    @Test
    void get_missingKey_returnsNull() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new GetCommand("nonexistentkey"));
        BaseResponse response = receiveResponse(client);

        assertInstanceOf(GetResponse.class, response, "Response should be GetResponse");
        GetResponse getResponse = (GetResponse) response;
        assertTrue(getResponse.isNull(), "Response should indicate null value");
        assertNull(getResponse.value(), "Value should be null for missing key");
        client.close();
    }

    @Test
    void multipleOperations_sameConnection() throws IOException {
        SocketChannel client = connectClient();

        // PUT
        sendCommand(client, new PutCommand("key1", "value1".getBytes(StandardCharsets.UTF_8)));
        BaseResponse putResponse1 = receiveResponse(client);
        assertInstanceOf(PutResponse.class, putResponse1);

        // GET
        sendCommand(client, new GetCommand("key1"));
        GetResponse getResponse1 = (GetResponse) receiveResponse(client);
        assertEquals("value1", new String(getResponse1.value(), StandardCharsets.UTF_8));

        // PUT overwrite
        sendCommand(client, new PutCommand("key1", "newvalue".getBytes(StandardCharsets.UTF_8)));
        BaseResponse putResponse2 = receiveResponse(client);
        assertInstanceOf(PutResponse.class, putResponse2);

        // GET updated
        sendCommand(client, new GetCommand("key1"));
        GetResponse getResponse2 = (GetResponse) receiveResponse(client);
        assertEquals("newvalue", new String(getResponse2.value(), StandardCharsets.UTF_8));

        client.close();
    }

    @Test
    void multipleKeys_allStored() throws IOException {
        SocketChannel client = connectClient();

        // Store multiple keys
        sendCommand(client, new PutCommand("keyA", "valueA".getBytes(StandardCharsets.UTF_8)));
        assertInstanceOf(PutResponse.class, receiveResponse(client));

        sendCommand(client, new PutCommand("keyB", "valueB".getBytes(StandardCharsets.UTF_8)));
        assertInstanceOf(PutResponse.class, receiveResponse(client));

        sendCommand(client, new PutCommand("keyC", "valueC".getBytes(StandardCharsets.UTF_8)));
        assertInstanceOf(PutResponse.class, receiveResponse(client));

        // Retrieve all
        sendCommand(client, new GetCommand("keyA"));
        GetResponse responseA = (GetResponse) receiveResponse(client);
        assertEquals("valueA", new String(responseA.value(), StandardCharsets.UTF_8));

        sendCommand(client, new GetCommand("keyB"));
        GetResponse responseB = (GetResponse) receiveResponse(client);
        assertEquals("valueB", new String(responseB.value(), StandardCharsets.UTF_8));

        sendCommand(client, new GetCommand("keyC"));
        GetResponse responseC = (GetResponse) receiveResponse(client);
        assertEquals("valueC", new String(responseC.value(), StandardCharsets.UTF_8));

        client.close();
    }

    @Test
    void mapIsUpdated_afterPut() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("maptestkey", "maptestvalue".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);

        // Verify directly in store
        KVMap.Node node = ((CommandHandler) server.getCommandHandler()).getStore().getMap().get("maptestkey");
        assertNotNull(node, "Key should exist in map");
        assertEquals("maptestvalue", new String(node.value(), StandardCharsets.UTF_8));

        client.close();
    }

    @Test
    void commandHandler_putCommand_storesValue() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        PutCommand command = new PutCommand("handlerKey", "handlerValue".getBytes(StandardCharsets.UTF_8));
        ByteBuffer result = handler.handleCommand(command);

        // Verify response
        result.rewind();
        BaseResponse response = BaseResponse.deserialize(result);
        assertInstanceOf(PutResponse.class, response, "Response should be PutResponse");

        // Verify value was stored
        KVMap.Node node = testStore.getMap().get("handlerKey");
        assertNotNull(node, "Key should exist in store");
        assertEquals("handlerValue", new String(node.value(), StandardCharsets.UTF_8));

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void commandHandler_getCommand_retrievesValue() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        // First store a value
        testStore.put("getKey", "getValue".getBytes(StandardCharsets.UTF_8));

        // Now retrieve it via command handler
        GetCommand command = new GetCommand("getKey");
        ByteBuffer result = handler.handleCommand(command);

        // Verify response
        result.rewind();
        BaseResponse response = BaseResponse.deserialize(result);
        assertInstanceOf(GetResponse.class, response, "Response should be GetResponse");
        GetResponse getResponse = (GetResponse) response;
        assertEquals("getValue", new String(getResponse.value(), StandardCharsets.UTF_8));

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void commandHandler_getMissingKey_returnsNull() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        // Try to get a non-existent key
        GetCommand command = new GetCommand("nonExistentKey");

        // Should return GetResponse with null value for missing key
        ByteBuffer result = handler.handleCommand(command);
        result.rewind();
        BaseResponse response = BaseResponse.deserialize(result);

        assertInstanceOf(GetResponse.class, response, "Response should be GetResponse");
        GetResponse getResponse = (GetResponse) response;
        assertTrue(getResponse.isNull(), "Response should indicate null value");
        assertNull(getResponse.value(), "Value should be null for missing key");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void commandHandler_putOverwritesExistingValue() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        // Store initial value
        PutCommand command1 = new PutCommand("overwriteKey", "initialValue".getBytes(StandardCharsets.UTF_8));
        handler.handleCommand(command1);

        // Overwrite with new value
        PutCommand command2 = new PutCommand("overwriteKey", "newValue".getBytes(StandardCharsets.UTF_8));
        ByteBuffer result = handler.handleCommand(command2);

        // Verify response
        result.rewind();
        BaseResponse response = BaseResponse.deserialize(result);
        assertInstanceOf(PutResponse.class, response, "Response should be PutResponse");

        // Verify value was overwritten
        KVMap.Node node = testStore.getMap().get("overwriteKey");
        assertEquals("newValue", new String(node.value(), StandardCharsets.UTF_8));

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void commandHandler_putEmptyValue_succeeds() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        // Store empty value
        PutCommand command = new PutCommand("emptyKey", new byte[0]);
        ByteBuffer result = handler.handleCommand(command);

        // Verify response
        result.rewind();
        BaseResponse response = BaseResponse.deserialize(result);
        assertInstanceOf(PutResponse.class, response, "Response should be PutResponse");

        // Verify value was stored
        KVMap.Node node = testStore.getMap().get("emptyKey");
        assertNotNull(node, "Key should exist in store");
        assertArrayEquals(new byte[0], node.value());

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void commandHandler_putBinaryValue_succeeds() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        // Store binary value
        byte[] binaryValue = new byte[]{0, 1, 2, 3, 127, (byte) 128, (byte) 255};
        PutCommand command = new PutCommand("binaryKey", binaryValue);
        ByteBuffer result = handler.handleCommand(command);

        // Verify response
        result.rewind();
        BaseResponse response = BaseResponse.deserialize(result);
        assertInstanceOf(PutResponse.class, response, "Response should be PutResponse");

        // Verify value was stored correctly
        KVMap.Node node = testStore.getMap().get("binaryKey");
        assertNotNull(node, "Key should exist in store");
        assertArrayEquals(binaryValue, node.value());

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void putCommand_serializeDeserialize_roundTrip() {
        String key = "testKey";
        byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);

        PutCommand original = new PutCommand(key, value);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(ByteBuffer.wrap(serialized));

        assertInstanceOf(PutCommand.class, deserialized, "Should deserialize to PutCommand");
        PutCommand putCommand = (PutCommand) deserialized;
        assertEquals(key, putCommand.key());
        assertArrayEquals(value, putCommand.value());
    }

    @Test
    void getCommand_serializeDeserialize_roundTrip() {
        String key = "testKey";

        GetCommand original = new GetCommand(key);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(ByteBuffer.wrap(serialized));

        assertInstanceOf(GetCommand.class, deserialized, "Should deserialize to GetCommand");
        GetCommand getCommand = (GetCommand) deserialized;
        assertEquals(key, getCommand.key());
    }

    @Test
    void putCommand_type_returnsPut() {
        PutCommand command = new PutCommand("key", "value".getBytes(StandardCharsets.UTF_8));
        assertEquals(CommandType.PUT, command.type());
    }

    @Test
    void getCommand_type_returnsGet() {
        GetCommand command = new GetCommand("key");
        assertEquals(CommandType.GET, command.type());
    }

    @Test
    void commandHandler_getStore_returnsCorrectStore() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        assertSame(testStore, handler.getStore(), "getStore should return the same store instance");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    // --- Response Serialization Tests ---

    @Test
    void putResponse_serializeDeserialize_roundTrip() {
        PutResponse original = new PutResponse();
        byte[] serialized = original.serialize();

        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        BaseResponse deserialized = BaseResponse.deserialize(buffer);

        assertInstanceOf(PutResponse.class, deserialized, "Should deserialize to PutResponse");
        assertEquals(ResponseType.PUT_RESPONSE, deserialized.type());
    }

    @Test
    void getResponse_serializeDeserialize_roundTrip() {
        byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);
        GetResponse original = new GetResponse(value);
        byte[] serialized = original.serialize();

        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        BaseResponse deserialized = BaseResponse.deserialize(buffer);

        assertInstanceOf(GetResponse.class, deserialized, "Should deserialize to GetResponse");
        GetResponse getResponse = (GetResponse) deserialized;
        assertArrayEquals(value, getResponse.value());
    }

    @Test
    void getResponse_nullValue_serializeDeserialize() {
        GetResponse original = new GetResponse(null);
        byte[] serialized = original.serialize();

        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        BaseResponse deserialized = BaseResponse.deserialize(buffer);

        assertInstanceOf(GetResponse.class, deserialized, "Should deserialize to GetResponse");
        GetResponse getResponse = (GetResponse) deserialized;
        assertTrue(getResponse.isNull(), "Should indicate null value");
        assertNull(getResponse.value(), "Value should be null");
    }

    @Test
    void errorResponse_serializeDeserialize_roundTrip() {
        ErrorResponse original = new ErrorResponse(ErrorType.SERVER_ERROR, "Test error message");
        byte[] serialized = original.serialize();

        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        BaseResponse deserialized = BaseResponse.deserialize(buffer);

        assertInstanceOf(ErrorResponse.class, deserialized, "Should deserialize to ErrorResponse");
        ErrorResponse errorResponse = (ErrorResponse) deserialized;
        assertEquals(ErrorType.SERVER_ERROR, errorResponse.errorType());
        assertEquals("Test error message", errorResponse.message());
    }

    // --- DELETE Unit Tests ---

    @Test
    void delete_existingKey_returnsTrue() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // First store a value
        testStore.put("deleteKey", "deleteValue".getBytes(StandardCharsets.UTF_8));

        // Verify key exists
        assertNotNull(testStore.get("deleteKey"));

        // Delete the key
        boolean result = testStore.delete("deleteKey");

        assertTrue(result, "Delete should return true for existing key");
        assertNull(testStore.get("deleteKey"), "Key should no longer exist after delete");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_nonExistingKey_returnsFalse() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Delete a non-existent key
        boolean result = testStore.delete("nonExistentKey");

        assertFalse(result, "Delete should return false for non-existing key");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_alreadyDeletedKey_returnsFalse() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store and delete a key
        testStore.put("tempKey", "tempValue".getBytes(StandardCharsets.UTF_8));
        testStore.delete("tempKey");

        // Try to delete again
        boolean result = testStore.delete("tempKey");

        assertFalse(result, "Delete should return false for already deleted key");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_emptyKey_returnsFalse() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Try to delete with empty key
        boolean result = testStore.delete("");

        assertFalse(result, "Delete should return false for empty key that doesn't exist");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_keyWithSpecialCharacters_succeeds() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store with special characters in key
        String specialKey = "key-with_special.chars:123!@#$%";
        testStore.put(specialKey, "value".getBytes(StandardCharsets.UTF_8));

        boolean result = testStore.delete(specialKey);

        assertTrue(result, "Delete should succeed for key with special characters");
        assertNull(testStore.get(specialKey), "Key should be removed");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_keyWithUnicode_succeeds() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store with unicode key
        String unicodeKey = "键值ストア🔑";
        testStore.put(unicodeKey, "value".getBytes(StandardCharsets.UTF_8));

        boolean result = testStore.delete(unicodeKey);

        assertTrue(result, "Delete should succeed for unicode key");
        assertNull(testStore.get(unicodeKey), "Key should be removed");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_onlyRemovesTargetKey() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store multiple keys
        testStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        testStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        testStore.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        // Delete middle key
        testStore.delete("key2");

        // Verify other keys still exist
        assertNotNull(testStore.get("key1"), "key1 should still exist");
        assertNotNull(testStore.get("key3"), "key3 should still exist");
        assertNull(testStore.get("key2"), "key2 should be deleted");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    // --- DELETE Integration Tests ---

    @Test
    void delete_afterPut_getReturnsNull() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        CommandHandler handler = new CommandHandler(testStore);

        // Store a value via command handler
        PutCommand putCommand = new PutCommand("integKey", "integValue".getBytes(StandardCharsets.UTF_8));
        handler.handleCommand(putCommand);

        // Verify it was stored
        GetCommand getCommand1 = new GetCommand("integKey");
        ByteBuffer result1 = handler.handleCommand(getCommand1);
        result1.rewind();
        GetResponse getResponse1 = (GetResponse) BaseResponse.deserialize(result1);
        assertEquals("integValue", new String(getResponse1.value(), StandardCharsets.UTF_8));

        // Delete directly from store
        testStore.delete("integKey");

        // Verify via command handler that key is gone
        GetCommand getCommand2 = new GetCommand("integKey");
        ByteBuffer result2 = handler.handleCommand(getCommand2);
        result2.rewind();
        GetResponse getResponse2 = (GetResponse) BaseResponse.deserialize(result2);
        assertTrue(getResponse2.isNull(), "Key should be deleted");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_multipleKeysSequentially() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store multiple keys
        testStore.put("del1", "value1".getBytes(StandardCharsets.UTF_8));
        testStore.put("del2", "value2".getBytes(StandardCharsets.UTF_8));
        testStore.put("del3", "value3".getBytes(StandardCharsets.UTF_8));

        // Delete all keys sequentially
        assertTrue(testStore.delete("del1"));
        assertTrue(testStore.delete("del2"));
        assertTrue(testStore.delete("del3"));

        // Verify all are gone
        assertNull(testStore.get("del1"));
        assertNull(testStore.get("del2"));
        assertNull(testStore.get("del3"));

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_afterOverwrite_succeeds() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store and overwrite
        testStore.put("overwriteDelKey", "initial".getBytes(StandardCharsets.UTF_8));
        testStore.put("overwriteDelKey", "overwritten".getBytes(StandardCharsets.UTF_8));

        // Delete should still work
        boolean result = testStore.delete("overwriteDelKey");

        assertTrue(result, "Delete should succeed for overwritten key");
        assertNull(testStore.get("overwriteDelKey"));

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_putAfterDelete_keyCanBeRestored() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store, delete, then store again
        testStore.put("restoreKey", "original".getBytes(StandardCharsets.UTF_8));
        testStore.delete("restoreKey");
        testStore.put("restoreKey", "restored".getBytes(StandardCharsets.UTF_8));

        // Verify key was restored with new value
        KVMap.Node node = testStore.get("restoreKey");
        assertNotNull(node, "Key should exist after re-put");
        assertEquals("restored", new String(node.value(), StandardCharsets.UTF_8));

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    @Test
    void delete_mapSizeDecreases() throws Exception {
        // Create a fresh store for this test
        Path testLogsFolder = Files.createTempDirectory("test-logs-");
        Path testSnapshotsFolder = Files.createTempDirectory("test-snapshots-");
        WALogger testLogger = new WALogger(testLogsFolder.resolve("app.log"));
        KVMapSnapshotter testSnapshotter = new KVMapSnapshotter(testSnapshotsFolder);

        KVStore testStore = new KVStore(new KVStore.Builder()
                .setLogHandler(new LogHandler(testLogger))
                .setSnapshotter(testSnapshotter));

        // Store a key
        testStore.put("sizeKey", "value".getBytes(StandardCharsets.UTF_8));
        int sizeBefore = testStore.getMap().size();

        // Delete the key
        testStore.delete("sizeKey");
        int sizeAfter = testStore.getMap().size();

        assertEquals(sizeBefore - 1, sizeAfter, "Map size should decrease by 1 after delete");

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    // --- DeleteCommand Tests ---

    @Test
    void deleteCommand_serializeDeserialize_roundTrip() {
        String key = "deleteTestKey";

        DeleteCommand original = new DeleteCommand(key);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(ByteBuffer.wrap(serialized));

        assertTrue(deserialized instanceof DeleteCommand, "Should deserialize to DeleteCommand");
        DeleteCommand deleteCommand = (DeleteCommand) deserialized;
        assertEquals(key, deleteCommand.key());
    }

    @Test
    void deleteCommand_type_returnsDelete() {
        DeleteCommand command = new DeleteCommand("key");
        assertEquals(CommandType.DELETE, command.type());
    }

    @Test
    void deleteCommand_withSpecialCharacters_serializesCorrectly() {
        String key = "key-with_special.chars:123";

        DeleteCommand original = new DeleteCommand(key);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(ByteBuffer.wrap(serialized));

        assertTrue(deserialized instanceof DeleteCommand);
        DeleteCommand deleteCommand = (DeleteCommand) deserialized;
        assertEquals(key, deleteCommand.key());
    }

    @Test
    void deleteCommand_withUnicode_serializesCorrectly() {
        String key = "削除キー🔑";

        DeleteCommand original = new DeleteCommand(key);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(ByteBuffer.wrap(serialized));

        assertTrue(deserialized instanceof DeleteCommand);
        DeleteCommand deleteCommand = (DeleteCommand) deserialized;
        assertEquals(key, deleteCommand.key());
    }

    @Test
    void deleteCommand_withEmptyKey_serializesCorrectly() {
        String key = "";

        DeleteCommand original = new DeleteCommand(key);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(ByteBuffer.wrap(serialized));

        assertTrue(deserialized instanceof DeleteCommand);
        DeleteCommand deleteCommand = (DeleteCommand) deserialized;
        assertEquals(key, deleteCommand.key());
    }
}
