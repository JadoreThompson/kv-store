package main.java.com.zenz.kvstore;

import com.zenz.kvstore.*;
import com.zenz.kvstore.commandHandlers.CommandHandler;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.GetCommand;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.LogHandler;
import com.zenz.kvstore.restorers.Restorer;
import org.junit.jupiter.api.*;

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

class KVConnectionManagerTest {

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

    private ByteBuffer receiveResponse(SocketChannel client) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int bytesRead = client.read(buffer);
        if (bytesRead <= 0) {
            return null;
        }
        buffer.flip();
        return buffer;
    }

    private String bufferToString(ByteBuffer buffer) {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new String(data, StandardCharsets.UTF_8).trim();
    }

    // --- Connection Tests ---

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

    // --- PUT Tests ---

    @Test
    void put_returnsOk() throws IOException {
        SocketChannel client = connectClient();
        sendCommand(client, new PutCommand("testkey", "testvalue".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer response = receiveResponse(client);

        assertEquals("OK", bufferToString(response));
        client.close();
    }

    @Test
    void put_storesInMap() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("mykey", "myvalue".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);  // consume OK

        // Verify via GET
        sendCommand(client, new GetCommand("mykey"));
        ByteBuffer response = receiveResponse(client);

        assertEquals("OK myvalue", bufferToString(response));
        client.close();
    }

    @Test
    void put_withSpacesInValue() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("greeting", "hello world".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);  // consume OK

        sendCommand(client, new GetCommand("greeting"));
        ByteBuffer response = receiveResponse(client);

        assertEquals("OK hello world", bufferToString(response));
        client.close();
    }

    // --- GET Tests ---

    @Test
    void get_existingKey_returnsValue() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("existingkey", "existingvalue".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);  // consume OK

        sendCommand(client, new GetCommand("existingkey"));
        ByteBuffer response = receiveResponse(client);

        assertEquals("OK existingvalue", bufferToString(response));
        client.close();
    }

    @Test
    void get_missingKey_returnsNull() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new GetCommand("nonexistentkey"));
        ByteBuffer response = receiveResponse(client);

        assertEquals("NULL", bufferToString(response));
        client.close();
    }

    // --- Multiple Operations Tests ---

    @Test
    void multipleOperations_sameConnection() throws IOException {
        SocketChannel client = connectClient();

        // PUT
        sendCommand(client, new PutCommand("key1", "value1".getBytes(StandardCharsets.UTF_8)));
        assertEquals("OK", bufferToString(receiveResponse(client)));

        // GET
        sendCommand(client, new GetCommand("key1"));
        assertEquals("OK value1", bufferToString(receiveResponse(client)));

        // PUT overwrite
        sendCommand(client, new PutCommand("key1", "newvalue".getBytes(StandardCharsets.UTF_8)));
        assertEquals("OK", bufferToString(receiveResponse(client)));

        // GET updated
        sendCommand(client, new GetCommand("key1"));
        assertEquals("OK newvalue", bufferToString(receiveResponse(client)));

        client.close();
    }

    @Test
    void multipleKeys_allStored() throws IOException {
        SocketChannel client = connectClient();

        // Store multiple keys
        sendCommand(client, new PutCommand("keyA", "valueA".getBytes(StandardCharsets.UTF_8)));
        assertEquals("OK", bufferToString(receiveResponse(client)));

        sendCommand(client, new PutCommand("keyB", "valueB".getBytes(StandardCharsets.UTF_8)));
        assertEquals("OK", bufferToString(receiveResponse(client)));

        sendCommand(client, new PutCommand("keyC", "valueC".getBytes(StandardCharsets.UTF_8)));
        assertEquals("OK", bufferToString(receiveResponse(client)));

        // Retrieve all
        sendCommand(client, new GetCommand("keyA"));
        assertEquals("OK valueA", bufferToString(receiveResponse(client)));

        sendCommand(client, new GetCommand("keyB"));
        assertEquals("OK valueB", bufferToString(receiveResponse(client)));

        sendCommand(client, new GetCommand("keyC"));
        assertEquals("OK valueC", bufferToString(receiveResponse(client)));

        client.close();
    }

    // --- Map Update Verification ---

    @Test
    void mapIsUpdated_afterPut() throws IOException {
        SocketChannel client = connectClient();

        sendCommand(client, new PutCommand("maptestkey", "maptestvalue".getBytes(StandardCharsets.UTF_8)));
        receiveResponse(client);

        // Verify directly in store
        KVMap.Node node = ((CommandHandler) server.getCommandHandler()).getStore().getMap().get("maptestkey");
        assertNotNull(node, "Key should exist in map");
        assertEquals("maptestvalue", new String(node.value, StandardCharsets.UTF_8));

        client.close();
    }

    // ============================================
    // CommandHandler Unit Tests
    // ============================================

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
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        assertEquals("OK", new String(resultBytes, StandardCharsets.UTF_8));

        // Verify value was stored
        KVMap.Node node = testStore.getMap().get("handlerKey");
        assertNotNull(node, "Key should exist in store");
        assertEquals("handlerValue", new String(node.value, StandardCharsets.UTF_8));

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
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        String response = new String(resultBytes, StandardCharsets.UTF_8);
        assertTrue(response.startsWith("OK "), "Response should start with 'OK '");
        assertTrue(response.contains("getValue"), "Response should contain the value");

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

        // Should return NULL for missing key
        ByteBuffer result = handler.handleCommand(command);
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        String response = new String(resultBytes, StandardCharsets.UTF_8);

        assertEquals("NULL", response, "Response should be NULL for missing key");

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
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        assertEquals("OK", new String(resultBytes, StandardCharsets.UTF_8));

        // Verify value was overwritten
        KVMap.Node node = testStore.getMap().get("overwriteKey");
        assertEquals("newValue", new String(node.value, StandardCharsets.UTF_8));

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
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        assertEquals("OK", new String(resultBytes, StandardCharsets.UTF_8));

        // Verify value was stored
        KVMap.Node node = testStore.getMap().get("emptyKey");
        assertNotNull(node, "Key should exist in store");
        assertArrayEquals(new byte[0], node.value);

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
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        assertEquals("OK", new String(resultBytes, StandardCharsets.UTF_8));

        // Verify value was stored correctly
        KVMap.Node node = testStore.getMap().get("binaryKey");
        assertNotNull(node, "Key should exist in store");
        assertArrayEquals(binaryValue, node.value);

        testLogger.close();
        testLogsFolder.toFile().delete();
        testSnapshotsFolder.toFile().delete();
    }

    // ============================================
    // Command Serialization/Deserialization Tests
    // ============================================

    @Test
    void putCommand_serializeDeserialize_roundTrip() {
        String key = "testKey";
        byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);

        PutCommand original = new PutCommand(key, value);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(serialized);

        assertTrue(deserialized instanceof PutCommand, "Should deserialize to PutCommand");
        PutCommand putCommand = (PutCommand) deserialized;
        assertEquals(key, putCommand.key());
        assertArrayEquals(value, putCommand.value());
    }

    @Test
    void getCommand_serializeDeserialize_roundTrip() {
        String key = "testKey";

        GetCommand original = new GetCommand(key);
        byte[] serialized = original.serialize();

        Command deserialized = Command.deserialize(serialized);

        assertTrue(deserialized instanceof GetCommand, "Should deserialize to GetCommand");
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
}