package com.zenz.kvstore.server;

import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.NodeState;
import com.zenz.kvstore.server.raft.RaftManager;
import com.zenz.kvstore.server.raft.RaftNode;
import com.zenz.kvstore.server.raft.messages.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for RaftControllerServer's handleLogRequest method.
 * Tests various scenarios for how the controller responds to LogRequests from follower brokers.
 * Tests connect to the RaftControllerServer server via network connections.
 */
class RaftControllerTest {

    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 6969;

    private RaftManager manager;
    private ArrayList<RaftNode> nodes;
    private Thread managerThread;
    private Path logsDir;
    private Path snapshotsDir;
    private KVMapSnapshotter snapshotter;
    private WALogger logger;
    private RaftLogHandler logHandler;
    private KVStore kvStore;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        logsDir = Files.createTempDirectory("logs-");
        snapshotsDir = Files.createTempDirectory("snapshots-");

        snapshotter = new KVMapSnapshotter(snapshotsDir);
        logger = new WALogger(logsDir.resolve("0.log"));
        logHandler = new RaftLogHandler(logger);
        kvStore = new KVStore(new KVStore.Builder()
                .setLogHandler(logHandler)
                .setSnapshotter(snapshotter)
        );

        nodes = new ArrayList<>();
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, TEST_PORT), null, NodeState.CONTROLLER));
        manager = new RaftManager(0, nodes, kvStore);
        startManager();

        Thread.sleep(500);
    }

    @AfterEach
    void afterEach() throws IOException, InterruptedException {
        logsDir.toFile().delete();
        snapshotsDir.toFile().delete();
        stopManager();
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

    private BaseMessage receiveMessage(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        channel.read(buffer);
        buffer.flip();
        BaseMessage response = BaseMessage.deserialize(buffer);
        return response;
    }

    private void startManager() {
        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the sever thread:");
                e.printStackTrace();
            }
        });
        managerThread.start();
    }

    private void stopManager() throws InterruptedException, IOException {
        manager.stop();
        managerThread.interrupt();
        managerThread.join(5000);
    }

    @Test
    @DisplayName("Fresh follower with both nodes having no processed commands")
    void freshFollower_bothNodesEmpty_returnsEmptyCommand() throws Exception {
        logHandler.setLogId(0);
        logHandler.setTerm(1);

        // Need to restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected log response");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(0, response.id());
        assertEquals(1, response.term());
        assertTrue(response.entries().isEmpty()); // No processed commands yet

        client.close();
    }

    @Test
    @DisplayName("Fresh follower when controller has snapshot - returns snapshot")
    void freshFollower_withSnapshot_returnsSnapshot() throws Exception {
        logHandler.setTerm(2);
        for (long i = 1; i <= 5; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        Path fpath = snapshotsDir.resolve(logHandler.getLogId() + "_" + logHandler.getTerm() + ".snapshot");
        snapshotter.snapshot(kvStore.getMap(), fpath);
        byte[] snapshotBytes = Files.readAllBytes(fpath);

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof InstallSnapshot, "Expected append snapshot message");
        InstallSnapshot response = (InstallSnapshot) resp;
        assertNotNull(response);
        assertNotNull(response.snapshot());
        assertArrayEquals(snapshotBytes, response.snapshot());

        client.close();
    }

    @Test
    @DisplayName("Fresh follower when controller has logs but no snapshot - returns first log")
    void freshFollower_noSnapshot_returnsFirstLog() throws Exception {
        // Setup: controller has processed some commands
        logHandler.setDisabled(false);
        logHandler.setTerm(1);
        kvStore.put("firstKey", "firstValue".getBytes(StandardCharsets.UTF_8));
        kvStore.put("secondKey", "secondValue".getBytes(StandardCharsets.UTF_8));
        kvStore.put("thirdKey", "thirdValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");

        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(1, response.id());
        assertEquals(1, response.term());
        assertFalse(response.entries().isEmpty());

        PutCommand cmd = (PutCommand) response.entries().get(0).command();
        assertEquals("firstKey", cmd.key());
        assertEquals("firstValue", new String(cmd.value(), StandardCharsets.UTF_8));

        client.close();
    }

    @Test
    @DisplayName("Follower up to date - same logId and term")
    void followerUpToDate_sameLogIdAndTerm_returnsMostRecentLog() throws Exception {
        // Setup
        logHandler.setTerm(2L);
        kvStore.put("firstKey", "firstValue".getBytes(StandardCharsets.UTF_8));
        kvStore.put("secondKey", "secondValue".getBytes(StandardCharsets.UTF_8));
        kvStore.put("thirdKey", "thirdValue".getBytes(StandardCharsets.UTF_8));

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(3, 2));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");

        client.close();
    }

    @Test
    @DisplayName("Same logId but different term - returns error")
    void sameLogIdDifferentTerm_returnsError() throws Exception {
        // Setup
        logHandler.setTerm(1L);
        for (long i = 1; i <= 5; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        // Request with same logId but different term
        sendMessage(client, new RequestEntry(5, 2));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof ErrorMessage, "Expected error response. Received " + resp);

        client.close();
    }

    /**
     * Testing that the controller returns a snapshot when the log id included
     * within the entry request is lower than the first log id within the current
     * log sequence.
     *
     * @throws Exception
     */
    @Test
    @DisplayName("Request logId before first log with snapshot - returns snapshot")
    void requestLogIdBeforeFirstLog_withSnapshot_returnsSnapshot() throws Exception {
        logHandler.setTerm(2);
        logHandler.setDisabled(false);
        for (int i = 1; i <= 5; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Create snapshot - this is required for the controller to have higher log ids
        Path snapshotPath = snapshotter.getDir().resolve(logHandler.getLogId() + "_" + logHandler.getTerm() + ".snapshot");
        snapshotter.snapshot(kvStore.getMap(), snapshotPath);

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        // Request with logId 0 (fresh follower) - should return snapshot
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof InstallSnapshot, "Expected log response");
        InstallSnapshot response = (InstallSnapshot) resp;

        assertNotNull(response);
        assertNotNull(response.snapshot());

        client.close();
    }

    @Test
    @DisplayName("Finding next log - returns correct next log entry")
    void findingNextLog_returnsCorrectNextLog() throws Exception {
        // Setup - logs start at id 1
        logHandler.setTerm(2);
        for (long i = 1; i <= 10; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        // Request with logId 5 - follower has processed log 5, needs log 6
        sendMessage(client, new RequestEntry(5, 2));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry. Received " + resp.getClass().getName());
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertFalse(response.entries().isEmpty());

        client.close();
    }

    @Test
    @DisplayName("Request with logId 0 but non-zero term")
    void logIdZeroWithNonZeroTerm() throws Exception {
        // Setup
        logHandler.setTerm(2);
        logHandler.setDisabled(false);
        kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        Path path = snapshotter.getDir().resolve(logHandler.getLogId() + "_" + logHandler.getTerm() + ".snapshot");
        snapshotter.snapshot(kvStore.getMap(), path);

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 2));

        // Log file is empty and the store is loaded from a snapshot
        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof InstallSnapshot, "Expected append snapshot. Received " + resp);

        client.close();
    }

    @Test
    @DisplayName("Multiple sequential requests from same client")
    void multipleSequentialRequests_sameClient() throws Exception {
        // Setup
        logHandler.setTerm(1);
        for (long i = 1; i <= 5; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected log response");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(1, response.id());

        sendMessage(client, new RequestEntry(1, 1));
        resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected log response");
        response = (AppendEntry) resp;
        assertNotNull(response);

        client.close();
    }

    @Test
    @DisplayName("Request with binary data in snapshot")
    void snapshotWithBinaryData() throws Exception {
        // Setup
        logHandler.setTerm(1);
        for (long i = 1; i <= 3; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Create snapshot with binary data
        byte[] binarySnapshot = new byte[]{0, 1, 2, 3, 127, (byte) 128, (byte) 255};
        Path snapshotPath = snapshotter.getDir().resolve(logHandler.getLogId() + "_" + logHandler.getTerm() + ".snapshot");
        Files.write(snapshotPath, binarySnapshot);

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof InstallSnapshot, "Expected append snapshot");
        InstallSnapshot response = (InstallSnapshot) resp;
        assertNotNull(response.snapshot());
        assertArrayEquals(binarySnapshot, response.snapshot());

        client.close();
    }

    @Test
    @DisplayName("Request with large snapshot")
    void largeSnapshot() throws Exception {
        // Setup
        logHandler.setTerm(1);
        for (long i = 1; i <= 3; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Create large snapshot (1KB)
        byte[] largeSnapshot = new byte[1024];
        for (int i = 0; i < largeSnapshot.length; i++) {
            largeSnapshot[i] = (byte) (i % 256);
        }
        Path snapshotPath = snapshotter.getDir().resolve(logHandler.getLogId() + "_" + logHandler.getTerm() + ".snapshot");
        Files.write(snapshotPath, largeSnapshot);

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();

        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof InstallSnapshot, "Expected append snapshot");
        InstallSnapshot response = (InstallSnapshot) resp;
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
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        // Should return empty command response
        BaseMessage resp = receiveMessage(client);
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
        logHandler.setTerm(1);
        kvStore.put("singleKey", "singleValue".getBytes(StandardCharsets.UTF_8));

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry. Received " + resp);
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertEquals(1, response.id());
        assertEquals(1, response.term());
        assertFalse(response.entries().isEmpty());
        PutCommand cmd = (PutCommand) response.entries().get(0).command();
        assertEquals("singleKey", cmd.key());

        client.close();
    }

    @Test
    @DisplayName("Request with different term values")
    void differentTermValues() throws Exception {
        // Setup with logs from different terms
        logHandler.setTerm(1);
        kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        kvStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        logHandler.setTerm(2);
        kvStore.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        kvStore.put("key4", "value4".getBytes(StandardCharsets.UTF_8));
        logHandler.setTerm(3);
        kvStore.put("key5", "value5".getBytes(StandardCharsets.UTF_8));

        // Restart controller to reload logs
        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client = connectClient();

        // Request with term 1
        sendMessage(client, new RequestEntry(2, 1));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");
        AppendEntry response = (AppendEntry) resp;
        assertNotNull(response);
        assertFalse(response.entries().isEmpty());

        client.close();
    }

    @Test
    @DisplayName("AppendEntryResponse updates session logId and term")
    void appendEntryResponse_updatesSessionState() throws Exception {
        // Setup - create logs so controller has state
        logHandler.setTerm(1);
        kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        kvStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect a follower and sync it first
        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry after initial request");

        // Now send an AppendEntryResponse to update session state
        sendMessage(client, new AppendEntryResponse(2, 1, true));

        // Give the server time to process
        Thread.sleep(500);

        // Verify the session was updated by making a new request
        // The follower should now be at logId 2, term 1
        sendMessage(client, new RequestEntry(2, 1));
        resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected append entry");
        AppendEntry response = (AppendEntry) resp;
        // Follower is up to date, should get empty commands list
        assertTrue(response.entries().isEmpty(), "Expected empty commands for up-to-date follower");

        client.close();
    }

    @Test
    @DisplayName("AppendEntryResponse with lower id triggers catch-up send")
    void appendEntryResponse_withLowerId_sendsCatchUpEntries() throws Exception {
        // Setup - create multiple logs
        logHandler.setTerm(1);
        kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        kvStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        kvStore.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        kvStore.put("key4", "value4".getBytes(StandardCharsets.UTF_8));

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect a follower
        SocketChannel client = connectClient();
        sendMessage(client, new RequestEntry(0, 0));

        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry);

        // Now send an AppendEntryResponse with id=1 (behind current logId=4)
        // This should trigger the controller to send catch-up entries
        sendMessage(client, new AppendEntryResponse(1, 1, true));

        // Receive the catch-up entries (logs 2, 3, 4)
        resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected catch-up append entry");
        AppendEntry response = (AppendEntry) resp;
        assertEquals(2, response.id(), "Expected catch-up to start from log 2");
        assertFalse(response.entries().isEmpty(), "Expected catch-up commands");

        client.close();
    }

    // Integration Tests for handleAppendEntryResponse

    @Test
    @DisplayName("Majority reached completes future")
    void majorityReached_completesFuture() throws Exception {
        // Setup - create logs
        logHandler.setTerm(1);
        kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect 3 followers (majority = 2)
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        // Sync all followers to current state
        sendMessage(client1, new RequestEntry(0, 0));
        sendMessage(client2, new RequestEntry(0, 0));
        sendMessage(client3, new RequestEntry(0, 0));

        receiveMessage(client1);
        receiveMessage(client2);
        receiveMessage(client3);

        // Now simulate a command broadcast by having followers at logId 1
        // Send responses from 2 followers (majority)
        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        Thread.sleep(50);
        sendMessage(client2, new AppendEntryResponse(1, 1, true));
        Thread.sleep(100);

        // Both followers should have their sessions updated
        sendMessage(client1, new RequestEntry(1, 1));
        BaseMessage resp1 = receiveMessage(client1);
        assertTrue(resp1 instanceof AppendEntry);

        sendMessage(client2, new RequestEntry(1, 1));
        BaseMessage resp2 = receiveMessage(client2);
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
        logHandler.setTerm(1);
        kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect multiple followers
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        // Sync all followers first
        sendMessage(client1, new RequestEntry(0, 0));
        sendMessage(client2, new RequestEntry(0, 0));
        sendMessage(client3, new RequestEntry(0, 0));

        receiveMessage(client1);
        receiveMessage(client2);
        receiveMessage(client3);

        // Simulate a command being handled (this would normally set up majority tracking)
        // For this test, we verify that responses with matching ids are handled
        sendMessage(client1, new AppendEntryResponse(1, 1, true));

        // Give server time to process
        Thread.sleep(100);

        // The session should be updated
        sendMessage(client1, new RequestEntry(1, 1));
        BaseMessage resp = receiveMessage(client1);
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
        logHandler.setTerm(1);
        for (long i = 1; i <= 5; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect a follower
        SocketChannel client = connectClient();

        // Initial sync - follower gets all logs
        sendMessage(client, new RequestEntry(0, 0));
        BaseMessage resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry);
        AppendEntry initialResponse = (AppendEntry) resp;
        assertEquals(1, initialResponse.id());

        // Follower responds that it only applied up to log 2 (behind)
        sendMessage(client, new AppendEntryResponse(2, 1, true));

        // Controller should send catch-up entries (logs 3, 4, 5)
        resp = receiveMessage(client);
        assertTrue(resp instanceof AppendEntry, "Expected catch-up entries");
        AppendEntry catchUpResponse = (AppendEntry) resp;
        assertEquals(3, catchUpResponse.id(), "Catch-up should start from log 3");
        assertFalse(catchUpResponse.entries().isEmpty(), "Expected catch-up commands");

        client.close();
    }

    @Test
    @DisplayName("Multiple followers with mixed states")
    void multipleFollowers_mixedStates_handledCorrectly() throws Exception {
        // Setup - create logs
        logHandler.setTerm(1);
        for (long i = 1; i <= 3; i++) {
            kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect 3 followers
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        // Sync all followers
        sendMessage(client1, new RequestEntry(0, 0));
        sendMessage(client2, new RequestEntry(0, 0));
        sendMessage(client3, new RequestEntry(0, 0));

        receiveMessage(client1);
        receiveMessage(client2);
        receiveMessage(client3);

        // Follower 1 responds with current logId (up to date)
        sendMessage(client1, new AppendEntryResponse(3, 1, true));
        Thread.sleep(50);

        // Follower 2 responds with older logId (behind)
        sendMessage(client2, new AppendEntryResponse(1, 1, true));
        Thread.sleep(50);

        // Follower 2 should receive catch-up entries
        BaseMessage resp2 = receiveMessage(client2);
        assertTrue(resp2 instanceof AppendEntry, "Follower 2 should get catch-up");
        AppendEntry catchUp = (AppendEntry) resp2;
        assertEquals(2, catchUp.id(), "Catch-up should start from log 2");

        // Follower 3 responds with current logId
        sendMessage(client3, new AppendEntryResponse(3, 1, true));
        Thread.sleep(50);

        // Verify all sessions are properly tracked
        sendMessage(client1, new RequestEntry(3, 1));
        BaseMessage resp1 = receiveMessage(client1);
        assertTrue(resp1 instanceof AppendEntry);

        client1.close();
        client2.close();
        client3.close();
    }
}
