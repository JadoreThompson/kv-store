package main.java.com.zenz.kvstore;

import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.WALogger;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.raft.messages.*;
import com.zenz.kvstore.requests.LogRequest;
import com.zenz.kvstore.raft.RaftController;
import com.zenz.kvstore.restorers.RaftRestorer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for RaftController's handleLogRequest method.
 * Tests various scenarios for how the controller responds to LogRequests from follower brokers.
 * Tests connect to the RaftController server via network connections.
 */
class RaftControllerTest {

    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 6969;

    private static ExecutorService serverExecutor;
    private static RaftController controller;
    private static Path tempDir;
    private static Path logsFolderPath;
    private static Path snapshotFolderPath;
    private static KVMapSnapshotter snapshotter;
    private static WALogger logger;
    private static RaftLogHandler logHandler;

    @BeforeAll
    static void startServer() throws Exception {
        tempDir = Files.createTempDirectory("raft-controller-test-");
        logsFolderPath = tempDir.resolve("logs");
        snapshotFolderPath = tempDir.resolve("snapshots");

        Files.createDirectories(logsFolderPath);
        Files.createDirectories(snapshotFolderPath);

        Path logPath = logsFolderPath.resolve("raft.log");
        logger = new WALogger(logPath);
        logHandler = new RaftLogHandler(logger);
        snapshotter = new KVMapSnapshotter(snapshotFolderPath);

        controller = new RaftController(TEST_HOST, TEST_PORT, logHandler, snapshotter);

        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            try {
                controller.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Wait for server to start
        Thread.sleep(1000);
    }

    @AfterAll
    static void stopServer() throws IOException {
        controller.stop();
        serverExecutor.shutdown();
        try {
            serverExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (logger != null) {
            logger.close();
        }
        // Cleanup temp directory
        tempDir.toFile().delete();
    }

    @BeforeEach
    void resetState() throws IOException {
        // Reset log handler state
        logHandler.setLogId(0);
        logHandler.setTerm(1);

        // Clear logs file
        Path logPath = logsFolderPath.resolve("raft.log");
        Files.deleteIfExists(logPath);
        Files.createFile(logPath);

        // Clear snapshots
        java.io.File[] snapshots = snapshotFolderPath.toFile().listFiles();
        if (snapshots != null) {
            for (java.io.File file : snapshots) {
                file.delete();
            }
        }
    }

    private SocketChannel connectClient() throws IOException {
        SocketChannel client = SocketChannel.open();
        client.configureBlocking(true);
        client.connect(new InetSocketAddress(TEST_HOST, TEST_PORT));
        return client;
    }

    private void sendMessage(SocketChannel channel, BaseMessage message) throws IOException {
        byte[] serialized = message.serialize();
        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private BaseMessage receiveResponse(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8192);

        channel.read(buffer);
        buffer.flip();

        BaseMessage response = BaseMessage.deserialize(buffer);
        return response;
    }

    /**
     * Creates logs in the log file and updates the log handler state.
     */
    private void createLogs(List<LogEntry> entries) throws IOException {
        logger.close();
        Path logPath = logsFolderPath.resolve("raft.log");
        Files.deleteIfExists(logPath);

        try (var channel = java.nio.channels.FileChannel.open(logPath,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE)) {
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
        // Update logHandler with new logger
        logHandler.setLogger(logger);

        // Set the last log id and term
        if (!entries.isEmpty()) {
            LogEntry last = entries.get(entries.size() - 1);
            logHandler.setLogId(last.id);
            logHandler.setTerm(last.term);
        }
    }

    /**
     * Creates a snapshot file with test data.
     */
    private void createSnapshot(byte[] data) throws IOException {
        Path snapshotPath = snapshotFolderPath.resolve("snapshot.dat");
        Files.write(snapshotPath, data);
    }

    private void restartController() throws Exception {
        // Stop the current controller
        controller.stop();
        Thread.sleep(200);

        // Create a new controller with the same configuration
        controller = new RaftController(TEST_HOST, TEST_PORT, logHandler, snapshotter);

        // Start in a new thread
        serverExecutor.shutdown();
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            try {
                controller.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Wait for server to start
        Thread.sleep(500);
    }

    /**
     * Helper record for log entry data.
     */
    private record LogEntry(long id, long term, String key, byte[] value) {
    }

    @Test
    @DisplayName("Client can connect to RaftController")
    void clientCanConnect() throws IOException {
        SocketChannel client = connectClient();
        assertTrue(client.isConnected(), "Client should be connected");
        client.close();
    }

    @Test
    @DisplayName("Multiple clients can connect to RaftController")
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
    @DisplayName("Fresh follower with both nodes having no processed commands")
    void freshFollower_bothNodesEmpty_returnsEmptyCommand() throws Exception {
        // Setup: currentLogId=0, currentTerm=1 (initial state)
        logHandler.setLogId(0);
        logHandler.setTerm(1);

        // Need to restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected log response");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(0, response.id());
        assertEquals(1, response.term());
//        assertNull(response.command()); // No processed commands yet
        assertTrue(response.commands().isEmpty()); // No processed commands yet

        client.close();
    }

    @Test
    @DisplayName("Fresh follower when controller has snapshot - returns snapshot")
    void freshFollower_withSnapshot_returnsSnapshot() throws Exception {
        // Setup: controller has processed some commands
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 5; i++) {
            entries.add(new LogEntry(i, 2, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        // Create snapshot
        byte[] snapshotData = "snapshot-data-content".getBytes(StandardCharsets.UTF_8);
        createSnapshot(snapshotData);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendSnapshot, "Expected append snapshot message");
        AppendSnapshot response = (AppendSnapshot) resp;
        assertNotNull(response);
        assertNotNull(response.snapshot());
        assertArrayEquals(snapshotData, response.snapshot());

        client.close();
    }

    @Test
    @DisplayName("Fresh follower when controller has logs but no snapshot - returns first log")
    void freshFollower_noSnapshot_returnsFirstLog() throws Exception {
        // Setup: controller has processed some commands
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "firstKey", "firstValue".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(2L, 1L, "secondKey", "secondValue".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(3L, 1L, "thirdKey", "thirdValue".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(1, response.id());
        assertEquals(1, response.term());
//        assertNotNull(response.command());
//        PutCommand cmd = (PutCommand) response.command();
        assertFalse(response.commands().isEmpty());
        PutCommand cmd = (PutCommand) response.commands().get(0);
        assertEquals("firstKey", cmd.key());
        assertEquals("firstValue", new String(cmd.value(), StandardCharsets.UTF_8));

        client.close();
    }

    @Test
    @DisplayName("Follower up to date - same logId and term")
    void followerUpToDate_sameLogIdAndTerm_returnsMostRecentLog() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 2L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(2L, 2L, "key2", "value2".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(3L, 2L, "key3", "value3".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(3, 2));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");

        client.close();
    }

    @Test
    @DisplayName("Same logId but different term - returns error")
    void sameLogIdDifferentTerm_returnsError() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 5; i++) {
            entries.add(new LogEntry(i, 3L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        // Request with same logId but different term
        sendMessage(client, new RequestEntry(5, 2));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof ErrorMessage, "Expected error response. Received " + resp);

        client.close();
    }

    @Test
    @DisplayName("Request logId before first log with snapshot - returns snapshot")
    void requestLogIdBeforeFirstLog_withSnapshot_returnsSnapshot() throws Exception {
        // Setup: Use RaftRestorer to properly create a store with logs and snapshot
        logHandler.setTerm(2);
        logHandler.setDisabled(false);
        KVStore.Builder builder = new KVStore.Builder();
        builder.setLogHandler(logHandler);
        builder.setSnapshotter(snapshotter);

        KVStore store = new RaftRestorer().restore(builder);

        // Add some data to create logs
        for (int i = 1; i <= 5; i++) {
            store.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Create snapshot - this is required for the controller to have higher log ids
        Path snapshotPath = snapshotter.getDir().resolve(logHandler.getLogId() + ".snapshot");
        snapshotter.snapshot(store.getMap(), snapshotPath);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        // Request with logId 0 (fresh follower) - should return snapshot
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendSnapshot, "Expected log response");
        AppendSnapshot response = (AppendSnapshot) resp;

        assertNotNull(response);
        assertNotNull(response.snapshot());

        client.close();
    }

    @Test
    @DisplayName("Finding next log - returns correct next log entry")
    void findingNextLog_returnsCorrectNextLog() throws Exception {
        // Setup - logs start at id 1
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 10; i++) {
            entries.add(new LogEntry(i, 2L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        // Request with logId 5 - follower has processed log 5, needs log 6
        sendMessage(client, new RequestEntry(5, 2));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected log response");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
//        assertNotNull(response.command());
        assertFalse(response.commands().isEmpty());

        client.close();
    }

    @Test
    @DisplayName("Finding next log - first log in sequence")
    void findingNextLog_firstLogInSequence() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(2L, 1L, "key2", "value2".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(3L, 1L, "key3", "value3".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        // Request with logId 1 - follower has processed log 1, needs log 2
        sendMessage(client, new RequestEntry(1, 1));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
//        assertNotNull(response.command());
        assertFalse(response.commands().isEmpty());

        client.close();
    }


    @Test
    @DisplayName("Terms match but log id is greater. Controller terminates before converting to follower.")
    void termsMatching_logIdLarge_terminatesController() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 5; i++) {
            entries.add(new LogEntry(i, 1L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(10, 1));

        Thread.sleep(500);
        assertTrue(!controller.isRunning(), "Controller should've stopped to convert itself to a follower");

        client.close();
    }

    @Test
    @DisplayName("Request with logId 0 but non-zero term")
    void logIdZeroWithNonZeroTerm() throws Exception {
        // Setup
        logHandler.setTerm(2);
        logHandler.setDisabled(false);
        KVStore.Builder builder = new KVStore.Builder();
        builder.setLogHandler(logHandler);
        builder.setSnapshotter(snapshotter);

        KVStore store = new RaftRestorer().restore(builder);
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        Path path = snapshotter.getDir().resolve(logHandler.getLogId() + ".snapshot");
        snapshotter.snapshot(store.getMap(), path);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 2));

        // Log file is empty and the store is loaded from a snapshot
        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendSnapshot, "Expected append snapshto. Received " + resp.toString());

        client.close();
    }

    @Test
    @DisplayName("Multiple sequential requests from same client")
    void multipleSequentialRequests_sameClient() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 5; i++) {
            entries.add(new LogEntry(i, 1L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected log response");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(1, response.id());

        sendMessage(client, new RequestEntry(1, 1));
        resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected log response");
        response = (AppendEntry) resp;
        assertNotNull(response);

        client.close();
    }

    @Test
    @DisplayName("Request with binary data in snapshot")
    void snapshotWithBinaryData() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 3; i++) {
            entries.add(new LogEntry(i, 1L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        // Create snapshot with binary data
        byte[] binarySnapshot = new byte[]{0, 1, 2, 3, 127, (byte) 128, (byte) 255};
        createSnapshot(binarySnapshot);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        ;
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendSnapshot, "Expected append snapshot");
        AppendSnapshot response = (AppendSnapshot) resp;
        assertNotNull(response.snapshot());
        assertArrayEquals(binarySnapshot, response.snapshot());

        client.close();
    }

    @Test
    @DisplayName("Request with large snapshot")
    void largeSnapshot() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 3; i++) {
            entries.add(new LogEntry(i, 1L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        // Create large snapshot (1KB)
        byte[] largeSnapshot = new byte[1024];
        for (int i = 0; i < largeSnapshot.length; i++) {
            largeSnapshot[i] = (byte) (i % 256);
        }
        createSnapshot(largeSnapshot);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();

        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendSnapshot, "Expected append snapshot");
        AppendSnapshot response = (AppendSnapshot) resp;
        assertNotNull(response.snapshot());
        assertArrayEquals(largeSnapshot, response.snapshot());

        client.close();
    }

    @Test
    @DisplayName("Request with empty logs and no snapshot")
    void emptyLogsNoSnapshot() throws Exception {
        // Setup - empty logs
        logHandler.setLogId(0);
        logHandler.setTerm(1);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        // Should return empty command response
        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(0, response.id());
        assertEquals(1, response.term());

        client.close();
    }

    @Test
    @DisplayName("Request with single log entry")
    void singleLogEntry() throws Exception {
        // Setup
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "singleKey", "singleValue".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry. Received " + resp);
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(1, response.id());
        assertEquals(1, response.term());
//        PutCommand cmd = (PutCommand) response.command();
        assertFalse(response.commands().isEmpty());
        PutCommand cmd = (PutCommand) response.commands().get(0);
        assertEquals("singleKey", cmd.key());

        client.close();
    }

    @Test
    @DisplayName("Request with different term values")
    void differentTermValues() throws Exception {
        // Setup with logs from different terms
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(2L, 1L, "key2", "value2".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(3L, 2L, "key3", "value3".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(4L, 2L, "key4", "value4".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(5L, 3L, "key5", "value5".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        // Restart controller to reload logs
        restartController();

        SocketChannel client = connectClient();

        // Request with term 1
        sendMessage(client, new RequestEntry(2, 1));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
//        assertNotNull(response.command());
        assertFalse(response.commands().isEmpty());

        client.close();
    }

    // ============================================
    // Unit Tests for handleAppendEntryResponse
    // ============================================

    @Test
    @DisplayName("AppendEntryResponse updates session logId and term")
    void appendEntryResponse_updatesSessionState() throws Exception {
        // Setup - create logs so controller has state
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(2L, 1L, "key2", "value2".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        restartController();

        // Connect a follower and sync it first
        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry after initial request");

        // Now send an AppendEntryResponse to update session state
        sendMessage(client, new AppendEntryResponse(2, 1, true));

        // Give the server time to process
        Thread.sleep(100);

        // Verify the session was updated by making a new request
        // The follower should now be at logId 2, term 1
        sendMessage(client, new RequestEntry(2, 1));
        resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");
        AppendEntry response = (AppendEntry) resp;
        // Follower is up to date, should get empty commands list
        assertTrue(response.commands().isEmpty(), "Expected empty commands for up-to-date follower");

        client.close();
    }

    @Test
    @DisplayName("AppendEntryResponse with lower id triggers catch-up send")
    void appendEntryResponse_withLowerId_sendsCatchUpEntries() throws Exception {
        // Setup - create multiple logs
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(2L, 1L, "key2", "value2".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(3L, 1L, "key3", "value3".getBytes(StandardCharsets.UTF_8)));
        entries.add(new LogEntry(4L, 1L, "key4", "value4".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        restartController();

        // Connect a follower
        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry);

        // Now send an AppendEntryResponse with id=1 (behind current logId=4)
        // This should trigger the controller to send catch-up entries
        sendMessage(client, new AppendEntryResponse(1, 1, true));

        // Receive the catch-up entries (logs 2, 3, 4)
        resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected catch-up append entry");
        AppendEntry response = (AppendEntry) resp;
        assertEquals(2, response.id(), "Expected catch-up to start from log 2");
        assertFalse(response.commands().isEmpty(), "Expected catch-up commands");

        client.close();
    }

    // ============================================
    // TODO: Integration Tests for handleAppendEntryResponse
    // ============================================

    @Test
    @DisplayName("Majority reached completes future")
    void majorityReached_completesFuture() throws Exception {
        // Setup - create logs
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        restartController();

        // Connect 3 followers (majority = 2)
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        // Sync all followers to current state
        sendMessage(client1, new RequestEntry(0, 0));
        sendMessage(client2, new RequestEntry(0, 0));
        sendMessage(client3, new RequestEntry(0, 0));

        receiveResponse(client1);
        receiveResponse(client2);
        receiveResponse(client3);

        // Now simulate a command broadcast by having followers at logId 1
        // Send responses from 2 followers (majority)
        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        Thread.sleep(50);
        sendMessage(client2, new AppendEntryResponse(1, 1, true));
        Thread.sleep(100);

        // Both followers should have their sessions updated
        sendMessage(client1, new RequestEntry(1, 1));
        BaseMessage resp1 = receiveResponse(client1);
        assertTrue(resp1 instanceof AppendEntry);

        sendMessage(client2, new RequestEntry(1, 1));
        BaseMessage resp2 = receiveResponse(client2);
        assertTrue(resp2 instanceof AppendEntry);

        client1.close();
        client2.close();
        client3.close();
    }

    @Test
    @DisplayName("AppendEntryResponse with matching id increments count for majority")
    @Disabled
    void appendEntryResponse_withMatchingId_incrementsCount() throws Exception {
        // Setup - create logs
        List<LogEntry> entries = new ArrayList<>();
        entries.add(new LogEntry(1L, 1L, "key1", "value1".getBytes(StandardCharsets.UTF_8)));
        createLogs(entries);

        restartController();

        // Connect multiple followers
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        // Sync all followers first
        sendMessage(client1, new RequestEntry(0, 0));
        sendMessage(client2, new RequestEntry(0, 0));
        sendMessage(client3, new RequestEntry(0, 0));

        receiveResponse(client1);
        receiveResponse(client2);
        receiveResponse(client3);

        // Simulate a command being handled (this would normally set up majority tracking)
        // For this test, we verify that responses with matching ids are handled
        sendMessage(client1, new AppendEntryResponse(1, 1, true));

        // Give server time to process
        Thread.sleep(100);

        // The session should be updated
        sendMessage(client1, new RequestEntry(1, 1));
        BaseMessage resp = receiveResponse(client1);
        assertTrue(resp instanceof AppendEntry);

        client1.close();
        client2.close();
        client3.close();
    }

    @Test
    @DisplayName("Follower behind receives catch-up entries via network")
    @Disabled
    void followerBehind_receivesCatchUpViaNetwork() throws Exception {
        // Setup - create multiple logs
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 5; i++) {
            entries.add(new LogEntry(i, 1L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        restartController();

        // Connect a follower
        SocketChannel client = connectClient();

        // Initial sync - follower gets all logs
        sendMessage(client, new RequestEntry(0, 0));
        BaseMessage resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry);
        AppendEntry initialResponse = (AppendEntry) resp;
        assertEquals(1, initialResponse.id());

        // Follower responds that it only applied up to log 2 (behind)
        sendMessage(client, new AppendEntryResponse(2, 1, true));

        // Controller should send catch-up entries (logs 3, 4, 5)
        resp = receiveResponse(client);
        assertTrue(resp instanceof AppendEntry, "Expected catch-up entries");
        AppendEntry catchUpResponse = (AppendEntry) resp;
        assertEquals(3, catchUpResponse.id(), "Catch-up should start from log 3");
        assertFalse(catchUpResponse.commands().isEmpty(), "Expected catch-up commands");

        client.close();
    }

    @Test
    @DisplayName("Multiple followers with mixed states")
    void multipleFollowers_mixedStates_handledCorrectly() throws Exception {
        // Setup - create logs
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 3; i++) {
            entries.add(new LogEntry(i, 1L, "key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
        createLogs(entries);

        restartController();

        // Connect 3 followers
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        // Sync all followers
        sendMessage(client1, new RequestEntry(0, 0));
        sendMessage(client2, new RequestEntry(0, 0));
        sendMessage(client3, new RequestEntry(0, 0));

        receiveResponse(client1);
        receiveResponse(client2);
        receiveResponse(client3);

        // Follower 1 responds with current logId (up to date)
        sendMessage(client1, new AppendEntryResponse(3, 1, true));
        Thread.sleep(50);

        // Follower 2 responds with older logId (behind)
        sendMessage(client2, new AppendEntryResponse(1, 1, true));
        Thread.sleep(50);

        // Follower 2 should receive catch-up entries
        BaseMessage resp2 = receiveResponse(client2);
        assertTrue(resp2 instanceof AppendEntry, "Follower 2 should get catch-up");
        AppendEntry catchUp = (AppendEntry) resp2;
        assertEquals(2, catchUp.id(), "Catch-up should start from log 2");

        // Follower 3 responds with current logId
        sendMessage(client3, new AppendEntryResponse(3, 1, true));
        Thread.sleep(50);

        // Verify all sessions are properly tracked
        sendMessage(client1, new RequestEntry(3, 1));
        BaseMessage resp1 = receiveResponse(client1);
        assertTrue(resp1 instanceof AppendEntry);

        client1.close();
        client2.close();
        client3.close();
    }
}
