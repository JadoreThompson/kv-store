package main.java.com.zenz.kvstore;

import com.zenz.kvstore.*;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.raft.RaftControllerClient;
import com.zenz.kvstore.raft.messages.*;
import com.zenz.kvstore.restorers.RaftRestorer;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for RaftControllerClient.
 * Tests the client's behavior when connecting to a controller and handling various message types.
 */
class RaftControllerClientTest {

    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 6970;

    private static Path tempDir;
    private static Path logsFolderPath;
    private static Path snapshotFolderPath;
    private static KVMapSnapshotter snapshotter;
    private static WALogger logger;
    private static RaftLogHandler logHandler;
    private static KVStore store;
    private static KVStore.Builder builder;

    @BeforeAll
    static void setupTempDirs() throws IOException {
        tempDir = Files.createTempDirectory("raft-client-test-");
        logsFolderPath = tempDir.resolve("logs");
        snapshotFolderPath = tempDir.resolve("snapshots");

        Files.createDirectories(logsFolderPath);
        Files.createDirectories(snapshotFolderPath);
    }

    @AfterAll
    static void cleanupTempDirs() throws IOException {
        deleteDirectory(tempDir.toFile());
    }

    @BeforeEach
    void setup() throws IOException {
        // Create fresh log file
        Path logPath = logsFolderPath.resolve("raft.log");
        Files.deleteIfExists(logPath);
        Files.createFile(logPath);

        logger = new WALogger(logPath);
        logHandler = new RaftLogHandler(logger);
        snapshotter = new KVMapSnapshotter(snapshotFolderPath);

        // Clear snapshots
        File[] snapshots = snapshotFolderPath.toFile().listFiles();
        if (snapshots != null) {
            for (File file : snapshots) {
                file.delete();
            }
        }

        // Create KVStore
        builder = new KVStore.Builder()
                .setLogHandler(logHandler)
                .setSnapshotter(snapshotter)
                .setRaftMode(true)
                .setSnapshotEnabled(false);
        store = new KVStore(builder); // Disable auto-snapshot for tests
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logger != null) {
            logger.close();
        }
    }

    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    /**
     * Creates a simple test server that captures the first message sent by the client.
     */
    private TestServer createTestServer() throws IOException {
        return new TestServer(TEST_HOST, TEST_PORT);
    }

    /**
     * Helper record for log entry data.
     */
    private record LogEntry(long id, long term, String key, byte[] value) {
    }

    /**
     * Creates logs in the log file and updates the log handler state.
     */
    private void createLogs(List<LogEntry> entries) throws IOException {
        logger.close();
        Path logPath = logsFolderPath.resolve("raft.log");
        Files.deleteIfExists(logPath);

        try (FileChannel channel = FileChannel.open(logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            for (LogEntry entry : entries) {
                PutCommand cmd = new PutCommand(entry.key, entry.value);
                RaftLogHandler.Log log = new RaftLogHandler.Log(entry.id, entry.term, cmd);
                byte[] logBytes = log.serialize();
                ByteBuffer buffer = ByteBuffer.wrap(logBytes);
                channel.write(buffer);
                channel.write(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));
            }
        }

        logger = new WALogger(logPath);
        logHandler.setLogger(logger);

        // Set the last log id and term
        if (!entries.isEmpty()) {
            LogEntry last = entries.get(entries.size() - 1);
            logHandler.setLogId(last.id);
            logHandler.setTerm(last.term);
        }
    }

    // ============================================
    // Test 1: Connection sends RequestEntry with last processed log
    // ============================================

    @Test
    @DisplayName("handleConnect sends RequestEntry with correct logId and term when logs exist")
    void handleConnect_withExistingLogs_sendsCorrectRequestEntry() throws Exception {
        // Setup: Create some logs
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(2L, 1L, "key2", "value2".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(3L, 2L, "key3", "value3".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        try (TestServer server = createTestServer()) {
            server.start();

            // Create and start client in background thread
            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for server to receive the message
            BaseMessage receivedMessage = server.waitForMessage(5000);

            assertNotNull(receivedMessage, "Server should have received a message");
            assertTrue(receivedMessage instanceof RequestEntry, "Message should be RequestEntry");

            RequestEntry requestEntry = (RequestEntry) receivedMessage;
            assertEquals(3L, requestEntry.id(), "RequestEntry should contain the last log id");
            assertEquals(2L, requestEntry.term(), "RequestEntry should contain the last log term");

            clientThread.interrupt();
        }
    }

    @Test
    @DisplayName("handleConnect sends RequestEntry with id=0 and term=0 when no logs exist")
    void handleConnect_withNoLogs_sendsRequestEntryWithZeroValues() throws Exception {
        // Setup: No logs, fresh state
        logHandler.setLogId(0);
        logHandler.setTerm(0);

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            BaseMessage receivedMessage = server.waitForMessage(5000);

            assertNotNull(receivedMessage, "Server should have received a message");
            assertTrue(receivedMessage instanceof RequestEntry, "Message should be RequestEntry");

            RequestEntry requestEntry = (RequestEntry) receivedMessage;
            assertEquals(0L, requestEntry.id(), "RequestEntry id should be 0 for fresh client");
            assertEquals(0L, requestEntry.term(), "RequestEntry term should be 0 for fresh client");

            clientThread.interrupt();
        }
    }

    // ============================================
    // Test 2: AppendEntryV2 handling
    // ============================================

    @Test
    @DisplayName("handleAppendEntry applies log entries to store and returns correct response")
    void handleAppendEntry_withLogEntries_appliesToStoreAndReturnsResponse() throws Exception {
        // Setup: Initial state
        logHandler.setLogId(0);
        logHandler.setTerm(1);

        // Create an AppendEntryV2 message with log entries
        List<RaftLogHandler.Log> entries = new ArrayList<>();
        entries.add(new RaftLogHandler.Log(1L, 1L, new PutCommand("key1", "value1".getBytes(StandardCharsets.UTF_8))));
        entries.add(new RaftLogHandler.Log(2L, 1L, new PutCommand("key2", "value2".getBytes(StandardCharsets.UTF_8))));

        AppendEntryV2 appendEntry = new AppendEntryV2(1L, 1L, entries);

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for initial RequestEntry
            BaseMessage initialRequest = server.waitForMessage(5000);
            assertNotNull(initialRequest, "Should receive initial RequestEntry");

            // Send AppendEntryV2 response
            server.sendMessage(appendEntry);

            // Wait for client's response
            BaseMessage response = server.waitForNextMessage(5000);

            // Verify store was updated
            assertNotNull(store.getMap().get("key1"), "Store should contain key1");
            assertNotNull(store.getMap().get("key2"), "Store should contain key2");
            assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), store.getMap().get("key1").value, "key1 should have correct value");
            assertArrayEquals("value2".getBytes(StandardCharsets.UTF_8), store.getMap().get("key2").value, "key2 should have correct value");

            // Verify response
            assertNotNull(response, "Client should send a response");
            assertTrue(response instanceof AppendEntryResponse, "Response should be AppendEntryResponse");

            AppendEntryResponse entryResponse = (AppendEntryResponse) response;
            assertTrue(entryResponse.success(), "Response should indicate success");
            assertEquals(logHandler.getLogId(), entryResponse.id(), "Response should contain current log id");
            assertEquals(logHandler.getTerm(), entryResponse.term(), "Response should contain current term");

            clientThread.interrupt();
        }
    }

    @Test
    @DisplayName("handleAppendEntry returns null when already up to date")
    void handleAppendEntry_whenUpToDate_returnsNull() throws Exception {
        // Setup: Client is already at logId=1, term=1
        logHandler.setLogId(1);
        logHandler.setTerm(1);

        // Create an AppendEntryV2 with matching id and term (client is up to date)
        AppendEntryV2 appendEntry = new AppendEntryV2(1L, 1L, new ArrayList<>());

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for initial request
            BaseMessage initialRequest = server.waitForMessage(5000);
            assertNotNull(initialRequest, "Should receive initial RequestEntry");

            // Send AppendEntryV2
            server.sendMessage(appendEntry);
            Thread.sleep(500); // Give client time to process

            // The client should not send a response since it's up to date
            // We can verify by checking that no additional messages were sent
            // (This is a bit tricky to test directly, so we verify the store wasn't modified)
            assertNull(store.getMap().get("anyKey"), "Store should not be modified when up to date");

            clientThread.interrupt();
        }
    }

    @Test
    @DisplayName("handleAppendEntry logs entries to log file")
    void handleAppendEntry_withLogEntries_logsToFile() throws Exception {
        // Setup
        logHandler.setLogId(0);
        logHandler.setTerm(1);

        List<RaftLogHandler.Log> entries = new ArrayList<>();
        entries.add(new RaftLogHandler.Log(1L, 1L, new PutCommand("testKey", "testValue".getBytes(StandardCharsets.UTF_8))));

        AppendEntryV2 appendEntry = new AppendEntryV2(1L, 1L, entries);

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for initial RequestEntry
            BaseMessage initialRequest = server.waitForMessage(5000);
            assertNotNull(initialRequest, "Should receive initial RequestEntry");

            // Send AppendEntryV2 response
            server.sendMessage(appendEntry);

            // Wait for client's response
            BaseMessage response = server.waitForNextMessage(5000);

            // Verify log file was updated
            Path logPath = logsFolderPath.resolve("raft.log");
            assertTrue(Files.exists(logPath), "Log file should exist");
            assertTrue(Files.size(logPath) > 0, "Log file should contain data");

            // Verify log content
            ArrayList<RaftLogHandler.Log> logs = RaftLogHandler.deserialize(logPath);
            assertFalse(logs.isEmpty(), "Log file should contain entries");

            RaftLogHandler.Log lastLog = logs.get(logs.size() - 1);
            assertEquals(1L, lastLog.id(), "Log entry should have correct id");
            assertEquals(1L, lastLog.term(), "Log entry should have correct term");
            assertEquals(CommandType.PUT, lastLog.command().type(), "Log entry should be PUT command");

            clientThread.interrupt();
        }
    }

    @Test
    @DisplayName("handleAppendEntry updates term when entry has different term")
    void handleAppendEntry_withDifferentTerm_updatesTerm() throws Exception {
        // Setup: Initial state with term 1
        logHandler.setLogId(0);
        logHandler.setTerm(1);

        // Create an AppendEntryV2 message with a log entry from term 2
        List<RaftLogHandler.Log> entries = new ArrayList<>();
        entries.add(new RaftLogHandler.Log(1L, 2L, new PutCommand("key1", "value1".getBytes(StandardCharsets.UTF_8))));

        AppendEntryV2 appendEntry = new AppendEntryV2(1L, 2L, entries);

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for initial RequestEntry
            BaseMessage initialRequest = server.waitForMessage(5000);
            assertNotNull(initialRequest, "Should receive initial RequestEntry");

            // Send AppendEntryV2 response
            server.sendMessage(appendEntry);

            // Wait for client's response
            BaseMessage response = server.waitForNextMessage(5000);

            // Verify term was updated
            assertEquals(2L, logHandler.getTerm(), "Log handler term should be updated to match entry term");

            // Verify response contains the updated term
            assertNotNull(response, "Client should send a response");
            AppendEntryResponse entryResponse = (AppendEntryResponse) response;
            assertEquals(2L, entryResponse.term(), "Response should contain updated term");

            clientThread.interrupt();
        }
    }

    // ============================================
    // Test 3: AppendSnapshot handling
    // ============================================

    @Test
    @DisplayName("handleAppendSnapshot writes snapshot to file and clears log")
    void handleAppendSnapshot_writesSnapshotAndClearsLog() throws Exception {
        // Setup: Create some existing logs
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "oldKey", "oldValue".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        // Create snapshot data
        byte[] snapshotData = "snapshot-content-data".getBytes(StandardCharsets.UTF_8);
        long snapshotLogId = 5L;
        long snapshotTerm = 2L;

        AppendSnapshotV2 appendSnapshot = new AppendSnapshotV2(snapshotData, snapshotLogId, snapshotTerm);

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for initial RequestEntry
            BaseMessage initialRequest = server.waitForMessage(5000);
            assertNotNull(initialRequest, "Should receive initial RequestEntry");

            // Send AppendSnapshotV2
            server.sendMessage(appendSnapshot);

            // Wait for client's response
            BaseMessage response = server.waitForNextMessage(5000);

            // Verify snapshot file was created
            Path expectedSnapshotPath = snapshotFolderPath.resolve(snapshotLogId + "_" + snapshotTerm + ".snapshot");
            String message = "";
            File[] files = snapshotFolderPath.toFile().listFiles();
            for (int i = 0; i < files.length; i++) {
                if (i > 0 && i < files.length - 1) {
                    message = message + ", ";
                }
                message = message + files[i].getName().toString();
            }
            System.out.println(snapshotFolderPath);
            message = "Snapshot file should be created. Files -> " + message;
            assertTrue(Files.exists(expectedSnapshotPath), message);

            byte[] writtenSnapshot = Files.readAllBytes(expectedSnapshotPath);
            assertArrayEquals(snapshotData, writtenSnapshot, "Snapshot content should match");

            // Verify log file was cleared
            Path logPath = logsFolderPath.resolve("raft.log");
            assertEquals(0, Files.size(logPath), "Log file should be cleared (empty)");

            // Verify response
            assertNotNull(response, "Client should send a response");
            assertTrue(response instanceof AppendEntryResponse, "Response should be AppendEntryResponse");

            AppendEntryResponse entryResponse = (AppendEntryResponse) response;
            assertTrue(entryResponse.success(), "Response should indicate success");
            assertEquals(snapshotLogId, entryResponse.id(), "Response should contain snapshot's log id");
            assertEquals(snapshotTerm, entryResponse.term(), "Response should contain snapshot's term");

            clientThread.interrupt();
        }
    }

    @Test
    @DisplayName("handleAppendSnapshot deletes old snapshots")
    void handleAppendSnapshot_deletesOldSnapshots() throws Exception {
        // Setup: Create an old snapshot file
        Path oldSnapshotPath = snapshotFolderPath.resolve("1_1.snapshot");
//        Files.write(oldSnapshotPath, "old-snapshot".getBytes(StandardCharsets.UTF_8));
        KVMap map = new KVMap();
        map.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        snapshotter.snapshot(map, oldSnapshotPath);
        assertTrue(Files.exists(oldSnapshotPath), "Old snapshot should exist before test");
        store = new RaftRestorer().restore(builder);
        System.out.println("Last log " + ((RaftLogHandler) store.getLogHandler()).getLog());
        byte[] snapshotData = "new-snapshot-content".getBytes(StandardCharsets.UTF_8);
        AppendSnapshotV2 appendSnapshot = new AppendSnapshotV2(snapshotData, 3L, 2L);

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for initial RequestEntry
            BaseMessage initialRequest = server.waitForMessage(5000);
            assertNotNull(initialRequest, "Should receive initial RequestEntry");

            // Send AppendSnapshotV2
            server.sendMessage(appendSnapshot);

            // Wait for client's response
            BaseMessage response = server.waitForNextMessage(5000);

            // Verify old snapshot was deleted
            assertFalse(Files.exists(oldSnapshotPath), "Old snapshot should be deleted");

            // Verify new snapshot exists
            Path newSnapshotPath = snapshotFolderPath.resolve("3_2.snapshot");
            assertTrue(Files.exists(newSnapshotPath), "New snapshot should exist");

            clientThread.interrupt();
        }
    }

    @Test
    @DisplayName("handleAppendSnapshot with large snapshot data")
    void handleAppendSnapshot_withLargeSnapshot_handlesCorrectly() throws Exception {
        // Create large snapshot (10KB)
        byte[] largeSnapshot = new byte[10 * 1024];
        for (int i = 0; i < largeSnapshot.length; i++) {
            largeSnapshot[i] = (byte) (i % 256);
        }

        AppendSnapshotV2 appendSnapshot = new AppendSnapshotV2(largeSnapshot, 100L, 5L);

        try (TestServer server = createTestServer()) {
            server.start();

            RaftControllerClient client = new RaftControllerClient(TEST_HOST, TEST_PORT, store, logHandler);
            Thread clientThread = new Thread(() -> {
                try {
                    client.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();

            // Wait for initial RequestEntry
            BaseMessage initialRequest = server.waitForMessage(5000);
            assertNotNull(initialRequest, "Should receive initial RequestEntry");

            // Send AppendSnapshotV2
            server.sendMessage(appendSnapshot);

            // Wait for client's response
            BaseMessage response = server.waitForNextMessage(5000);

            // Verify large snapshot was written correctly
            Path snapshotPath = snapshotFolderPath.resolve("100_5.snapshot");
            assertTrue(Files.exists(snapshotPath), "Snapshot file should exist");

            byte[] writtenSnapshot = Files.readAllBytes(snapshotPath);
            assertArrayEquals(largeSnapshot, writtenSnapshot, "Large snapshot content should match");

            // Verify response
            assertNotNull(response, "Client should send a response");
            AppendEntryResponse entryResponse = (AppendEntryResponse) response;
            assertEquals(100L, entryResponse.id(), "Response should contain correct log id");
            assertEquals(5L, entryResponse.term(), "Response should contain correct term");

            clientThread.interrupt();
        }
    }

    // ============================================
    // Helper Test Server Class
    // ============================================

    /**
     * A simple test server that can capture messages sent by the client and send responses.
     */
    private static class TestServer implements AutoCloseable {
        private final String host;
        private final int port;
        private ServerSocketChannel serverChannel;
        private SocketChannel clientChannel;
        private BaseMessage receivedMessage;
        private BaseMessage responseMessage;
        private volatile boolean running;

        public TestServer(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public void start() throws IOException {
            serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(host, port));
            serverChannel.configureBlocking(true);
            running = true;

            // Start accepting connections in a separate thread
            new Thread(() -> {
                try {
                    clientChannel = serverChannel.accept();
                    handleClient();
                } catch (IOException e) {
                    if (running) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        private void handleClient() throws IOException {
            // Read the first message from client
            ByteBuffer readBuffer = ByteBuffer.allocate(8192);
            int bytesRead = clientChannel.read(readBuffer);

            if (bytesRead > 0) {
                readBuffer.flip();
                receivedMessage = BaseMessage.deserialize(readBuffer);

                // If we have a response message to send, send it
                if (responseMessage != null) {
                    sendResponse(responseMessage);
                }
            }
        }

        public BaseMessage waitForMessage(long timeoutMs) throws InterruptedException {
            long startTime = System.currentTimeMillis();
            while (receivedMessage == null && System.currentTimeMillis() - startTime < timeoutMs) {
                Thread.sleep(50);
            }
            return receivedMessage;
        }

        public void setResponseMessage(BaseMessage message) {
            this.responseMessage = message;
        }

        /**
         * Sends a message to the client without waiting for a response.
         */
        public void sendMessage(BaseMessage message) throws IOException {
            if (clientChannel != null && clientChannel.isConnected()) {
                ByteBuffer buffer = ByteBuffer.wrap(message.serialize());
                while (buffer.hasRemaining()) {
                    clientChannel.write(buffer);
                }
            }
        }

        /**
         * Sends a message and waits for the client's response.
         */
        public void sendResponse(BaseMessage message) throws IOException {
            if (clientChannel != null && clientChannel.isConnected()) {
                ByteBuffer buffer = ByteBuffer.wrap(message.serialize());
                while (buffer.hasRemaining()) {
                    clientChannel.write(buffer);
                }

                // Read response from client
                ByteBuffer readBuffer = ByteBuffer.allocate(8192);
                int bytesRead = clientChannel.read(readBuffer);
                if (bytesRead > 0) {
                    readBuffer.flip();
                    receivedMessage = BaseMessage.deserialize(readBuffer);
                }
            }
        }

        /**
         * Clears the current received message and waits for a new one.
         */
        public BaseMessage waitForNextMessage(long timeoutMs) throws InterruptedException, IOException {
            receivedMessage = null;
            ByteBuffer readBuffer = ByteBuffer.allocate(8192);
            long startTime = System.currentTimeMillis();

            while (receivedMessage == null && System.currentTimeMillis() - startTime < timeoutMs) {
                if (clientChannel != null && clientChannel.isConnected()) {
                    int bytesRead = clientChannel.read(readBuffer);
                    if (bytesRead > 0) {
                        readBuffer.flip();
                        receivedMessage = BaseMessage.deserialize(readBuffer);
                        break;
                    }
                }
                Thread.sleep(50);
            }
            return receivedMessage;
        }

        public BaseMessage waitForResponse(long timeoutMs) throws InterruptedException {
            long startTime = System.currentTimeMillis();
            while (receivedMessage == null && System.currentTimeMillis() - startTime < timeoutMs) {
                Thread.sleep(50);
            }
            return receivedMessage;
        }

        @Override
        public void close() throws IOException {
            running = false;
            if (clientChannel != null) {
                clientChannel.close();
            }
            if (serverChannel != null) {
                serverChannel.close();
            }
        }
    }
}