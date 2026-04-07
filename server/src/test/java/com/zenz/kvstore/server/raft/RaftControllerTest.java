package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class RaftControllerTest {

    private static final int TEST_PORT = 6969;

    private Manager managerNew;

    private ArrayList<RaftNodeConfig> nodes;

    private NodeConfig nodeConfig;

    private Thread managerThread;

    private Path logsDir;

    private Path snapshotsDir;

    private KVMapSnapshotter snapshotter;

    private WALogger logger;

    private RaftLogHandler logHandler;

    private KVStore kvStore;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        this.logsDir = Files.createTempDirectory("logs-");
        this.snapshotsDir = Files.createTempDirectory("snapshots-");

        this.snapshotter = new KVMapSnapshotter(this.snapshotsDir);
        this.logger = new WALogger(this.logsDir.resolve("0.log"));
        this.logHandler = new RaftLogHandler(this.logger);
        this.kvStore = new KVStore(new KVStore.Builder()
                .setLogHandler(this.logHandler)
                .setSnapshotter(this.snapshotter)
        );

        this.nodes = new ArrayList<>();
        this.nodes.add(new RaftNodeConfig(
                0, new InetSocketAddress("localhost", TEST_PORT), null, NodeRole.CONTROLLER));
        this.nodeConfig = new NodeConfig(
                "test-node", new InetSocketAddress("localhost", TEST_PORT), null);
        Thread.sleep(500);
    }

    @AfterEach
    void afterEach() throws IOException, InterruptedException {
        this.logsDir.toFile().delete();
        this.snapshotsDir.toFile().delete();
        stopManagerNew();
    }

    private void startManager() {
        this.managerNew = new Manager(this.kvStore, this.nodeConfig, Collections.emptyList());
        this.managerThread = new Thread(() -> {
            try {
                this.managerNew.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the sever thread:");
                e.printStackTrace();
            }
        });
        this.managerThread.start();
    }

    private void stopManagerNew() throws InterruptedException, IOException {
        this.managerNew.stop();
        this.managerThread.interrupt();
        this.managerThread.join(5000);
    }

    @Test
    @DisplayName("Fresh follower with both nodes having no processed commands")
    void freshFollower_bothNodesEmpty_returnsEmptyCommand() throws Exception {
        this.logHandler.setLogId(0);
        this.logHandler.setTerm(1);
        startManager();
        Thread.sleep(500);

        this.managerNew.setRole(NodeRole.CONTROLLER);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            Utils.sendMessage(client, new RequestEntry(0, 0));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected log response");

            final AppendEntry response = (AppendEntry) resp;
            assertNotNull(response);
            assertEquals(0, response.id());
            assertEquals(1, response.term());
            assertTrue(response.entries().isEmpty()); // No processed commands yet
        }
    }

    @Test
    @DisplayName("Fresh follower when controller has snapshot - returns snapshot")
    void freshFollower_withSnapshot_returnsSnapshot() throws Exception {
        this.logHandler.setTerm(2);
        for (long i = 1; i <= 5; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        final Path fpath = this.snapshotsDir.resolve(this.logHandler.getLogId() + "_" + this.logHandler.getTerm() + ".snapshot");
        this.snapshotter.snapshot(this.kvStore.getMap(), fpath);
        final byte[] snapshotBytes = Files.readAllBytes(fpath);

        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(InstallSnapshot.class, resp, "Expected append snapshot message");

            final InstallSnapshot response = (InstallSnapshot) resp;
            assertNotNull(response);
            assertNotNull(response.snapshot());
            assertArrayEquals(snapshotBytes, response.snapshot());
        }
    }

    @Test
    @DisplayName("Fresh follower when controller has logs but no snapshot - returns first log")
    void freshFollower_noSnapshot_returnsFirstLog() throws Exception {
        this.logHandler.setDisabled(false);
        this.logHandler.setTerm(1);
        this.kvStore.put("firstKey", "firstValue".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("secondKey", "secondValue".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("thirdKey", "thirdValue".getBytes(StandardCharsets.UTF_8));
        logger.close();

        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry");

            final AppendEntry response = (AppendEntry) resp;
            assertNotNull(response);
            assertEquals(1, response.id());
            assertEquals(1, response.term());
            assertFalse(response.entries().isEmpty());

            final PutCommand cmd = (PutCommand) response.entries().getFirst().command();
            assertEquals("firstKey", cmd.key());
            assertEquals("firstValue", new String(cmd.value(), StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("Follower up to date - same prevLogId and term")
    void followerUpToDate_sameLogIdAndTerm_returnsMostRecentLog() throws Exception {
        // Setup
        this.logHandler.setTerm(2L);
        this.kvStore.put("firstKey", "firstValue".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("secondKey", "secondValue".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("thirdKey", "thirdValue".getBytes(StandardCharsets.UTF_8));

        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(3, 2));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry");

        }
    }

    @Test
    @DisplayName("Same prevLogId but different term - returns error")
    void sameLogIdDifferentTerm_returnsError() throws Exception {
        // Setup
        this.logHandler.setTerm(1L);
        for (long i = 1; i <= 5; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(5, 2));

            Thread.sleep(500);
            assertEquals(
                    NodeRole.BROKER,
                    this.managerNew.getRole(),
                    "Manager should step down as controller to broker when receiving request entry with greater term"
            );
        }
    }

    /**
     * Testing that the controller returns a snapshot when the log id included
     * within the entry request is lower than the first log id within the current
     * log sequence.
     *
     * @throws Exception
     */
    @Test
    @DisplayName("Request prevLogId before first log with snapshot - returns snapshot")
    void requestLogIdBeforeFirstLog_withSnapshot_returnsSnapshot() throws Exception {
        this.logHandler.setTerm(2);
        this.logHandler.setDisabled(false);
        for (int i = 1; i <= 5; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Create snapshot - this is required for the controller to have higher log ids
        final Path snapshotPath = this.snapshotter.getDir().resolve(this.logHandler.getLogId() + "_" + this.logHandler.getTerm() + ".snapshot");
        this.snapshotter.snapshot(this.kvStore.getMap(), snapshotPath);

        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(InstallSnapshot.class, resp, "Expected log response");

            final InstallSnapshot response = (InstallSnapshot) resp;
            assertNotNull(response);
            assertNotNull(response.snapshot());
        }
    }

    @Test
    @DisplayName("Finding next log - returns correct next log entry")
    void findingNextLog_returnsCorrectNextLog() throws Exception {
        // Setup - logs start at id 1
        this.logHandler.setTerm(2);
        for (long i = 1; i <= 10; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(5, 2));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry. Received " + resp.getClass().getName());

            final AppendEntry response = (AppendEntry) resp;
            assertNotNull(response);
            assertFalse(response.entries().isEmpty());
        }
    }

    @Test
    @DisplayName("Request with prevLogId 0 but non-zero term")
    void logIdZeroWithNonZeroTerm() throws Exception {
        this.logHandler.setTerm(2);
        this.logHandler.setDisabled(false);
        this.kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        final Path path = this.snapshotter.getDir().resolve(this.logHandler.getLogId() + "_" + this.logHandler.getTerm() + ".snapshot");
        this.snapshotter.snapshot(this.kvStore.getMap(), path);

        // Restart controller to reload logs
        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 2));

            // Log file is empty and the store is loaded from a snapshot
            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(InstallSnapshot.class, resp, "Expected append snapshot. Received " + resp);
        }
    }

    @Test
    @DisplayName("Multiple sequential requests from same client")
    void multipleSequentialRequests_sameClient() throws Exception {
        // Setup
        this.logHandler.setTerm(1);
        for (long i = 1; i <= 5; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Restart controller to reload logs
        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected log response");
            AppendEntry response = (AppendEntry) resp;
            assertNotNull(response);
            assertEquals(1, response.id());

            Utils.sendMessage(client, new RequestEntry(1, 1));
            resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected log response");
            response = (AppendEntry) resp;
            assertNotNull(response);
        }
    }

    @Test
    @DisplayName("Request with binary data in snapshot")
    void snapshotWithBinaryData() throws Exception {
        // Setup
        this.logHandler.setTerm(1);
        for (long i = 1; i <= 3; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Create snapshot with binary data
        byte[] binarySnapshot = new byte[]{0, 1, 2, 3, 127, (byte) 128, (byte) 255};
        final Path snapshotPath = this.snapshotter.getDir().resolve(
                this.logHandler.getLogId() + "_" + this.logHandler.getTerm() + ".snapshot");
        Files.write(snapshotPath, binarySnapshot);

        // Restart controller to reload logs
        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(InstallSnapshot.class, resp, "Expected append snapshot");

            final InstallSnapshot response = (InstallSnapshot) resp;
            assertNotNull(response.snapshot());
            assertArrayEquals(binarySnapshot, response.snapshot());
        }
    }

    @Test
    @DisplayName("Request with large snapshot")
    void largeSnapshot() throws Exception {
        // Setup
        this.logHandler.setTerm(1);
        for (long i = 1; i <= 3; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Create large snapshot (1KB)
        byte[] largeSnapshot = new byte[1024];
        for (int i = 0; i < largeSnapshot.length; i++) {
            largeSnapshot[i] = (byte) (i % 256);
        }
        final Path snapshotPath = this.snapshotter.getDir().resolve(
                this.logHandler.getLogId() + "_" + this.logHandler.getTerm() + ".snapshot");
        Files.write(snapshotPath, largeSnapshot);

        // Restart controller to reload logs
        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);

            Utils.sendMessage(client, new RequestEntry(0, 0));
            Thread.sleep(500);
            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(InstallSnapshot.class, resp, "Expected install snapshot");

            final InstallSnapshot response = (InstallSnapshot) resp;
            assertNotNull(response.snapshot());
            assertArrayEquals(largeSnapshot, response.snapshot());
        }
    }

    @Test
    @DisplayName("Request with empty logs and no snapshot")
    void emptyLogsNoSnapshot() throws Exception {
        // Setup - empty logs
        this.logHandler.setLogId(0);
        this.logHandler.setTerm(1);

        // Restart controller to reload logs
        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            // Should return empty command response
            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry");

            final AppendEntry response = (AppendEntry) resp;
            assertNotNull(response);
            assertEquals(0, response.id());
            assertEquals(1, response.term());
        }
    }

    @Test
    @DisplayName("Request with single log entry")
    void singleLogEntry() throws Exception {
        // Setup
        this.logHandler.setTerm(1);
        this.kvStore.put("singleKey", "singleValue".getBytes(StandardCharsets.UTF_8));

        // Restart controller to reload logs
        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry. Received " + resp);

            final AppendEntry response = (AppendEntry) resp;
            assertNotNull(response);
            assertEquals(1, response.id());
            assertEquals(1, response.term());
            assertFalse(response.entries().isEmpty());

            final PutCommand cmd = (PutCommand) response.entries().getFirst().command();
            assertEquals("singleKey", cmd.key());
        }
    }

    @Test
    @DisplayName("Request with different term values")
    void differentTermValues() throws Exception {
        // Setup with logs from different terms
        this.logHandler.setTerm(1);
        this.kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        this.logHandler.setTerm(2);
        this.kvStore.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("key4", "value4".getBytes(StandardCharsets.UTF_8));
        this.logHandler.setTerm(3);
        this.kvStore.put("key5", "value5".getBytes(StandardCharsets.UTF_8));

        // Restart controller to reload logs
        startManager();
        Thread.sleep(500);

        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);

            // Request with term 1
            Utils.sendMessage(client, new RequestEntry(2, 1));

            final Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry");

            final AppendEntry response = (AppendEntry) resp;
            assertNotNull(response);
            assertFalse(response.entries().isEmpty());
        }
    }

    @Test
    @DisplayName("AppendEntryResponse updates session prevLogId and term")
    void appendEntryResponse_updatesSessionState() throws Exception {
        // Setup - create logs so controller has state
        this.logHandler.setTerm(1);
        this.kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        startManager();
        Thread.sleep(500);

        // Connect a follower and sync it first
        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry after initial request");

            Utils.sendMessage(client, new AppendEntryResponse(2, 1, true));
            Thread.sleep(500);

            // Verify the session was updated by making a new request
            // The follower should now be at prevLogId 2, term 1
            Utils.sendMessage(client, new RequestEntry(2, 1));
            resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected append entry");

            final AppendEntry response = (AppendEntry) resp;
            assertTrue(response.entries().isEmpty(), "Expected empty commands for up-to-date follower");
        }
    }

    @Test
    @DisplayName("AppendEntryResponse with lower id sends catch-up entries")
    void appendEntryResponse_withLowerId_sendsCatchUpEntries() throws Exception {
        // Setup - create multiple logs
        this.logHandler.setTerm(1);
        this.kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("key4", "value4".getBytes(StandardCharsets.UTF_8));

        startManager();
        Thread.sleep(500);

        // Connect a follower
        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);
            Utils.sendMessage(client, new RequestEntry(0, 0));

            Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp);
            // Now send an AppendEntryResponse with id=1 (behind current prevLogId=4)
            // This should trigger the controller to send catch-up entries
            Utils.sendMessage(client, new AppendEntryResponse(1, 1, true));

            // Receive the catch-up entries (logs 2, 3, 4)
            resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected catch-up append entry");

            final AppendEntry response = (AppendEntry) resp;
            assertEquals(2, response.id(), "Expected catch-up to start from log 2");
            assertFalse(response.entries().isEmpty(), "Expected catch-up commands");
        }
    }

    // Integration Tests for handleAppendEntryResponse

    @Test
    @DisplayName("Majority reached completes future")
    void majorityReached_completesFuture() throws Exception {
        // Setup - create logs
        this.logHandler.setTerm(1);
        this.kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        startManager();
        Thread.sleep(500);

        // Connect 3 followers (majority = 2)
        try (SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
             SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
             SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);

            // Sync all followers to current state
            Utils.sendMessage(client1, new RequestEntry(0, 0));
            Utils.sendMessage(client2, new RequestEntry(0, 0));
            Utils.sendMessage(client3, new RequestEntry(0, 0));

            Utils.receiveMessage(client1);
            Utils.receiveMessage(client2);
            Utils.receiveMessage(client3);

            // Now simulate a command broadcast by having followers at prevLogId 1
            // Send responses from 2 followers (majority)
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Thread.sleep(50);
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));
            Thread.sleep(100);

            // Both followers should have their sessions updated
            Utils.sendMessage(client1, new RequestEntry(1, 1));
            final Message resp1 = Utils.receiveMessage(client1);
            assertInstanceOf(AppendEntry.class, resp1);

            Utils.sendMessage(client2, new RequestEntry(1, 1));
            final Message resp2 = Utils.receiveMessage(client2);
            assertInstanceOf(AppendEntry.class, resp2);
        }
    }

    @Test
    @DisplayName("AppendEntryResponse with matching id increments count for majority")
    @Disabled
    void appendEntryResponse_withMatchingId_incrementsCount() throws Exception {
        // Setup - create logs
        this.logHandler.setTerm(1);
        this.kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        startManager();
        Thread.sleep(500);

        // Connect multiple followers
        try (SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
             SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
             SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress())) {

            this.managerNew.setRole(NodeRole.CONTROLLER);

            // Sync all followers first
            Utils.sendMessage(client1, new RequestEntry(0, 0));
            Utils.sendMessage(client2, new RequestEntry(0, 0));
            Utils.sendMessage(client3, new RequestEntry(0, 0));

            Utils.receiveMessage(client1);
            Utils.receiveMessage(client2);
            Utils.receiveMessage(client3);

            // Simulate a command being handled (this would normally set up majority tracking)
            // For this test, we verify that responses with matching ids are handled
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));

            // Give server time to process
            Thread.sleep(100);

            // The session should be updated
            Utils.sendMessage(client1, new RequestEntry(1, 1));
            final Message resp = Utils.receiveMessage(client1);
            assertInstanceOf(AppendEntry.class, resp);
        }
    }

    @Test
    @DisplayName("Follower behind receives catch-up entries via network")
    @Disabled
    void followerBehind_receivesCatchUpViaNetwork() throws Exception {
        // Setup - create multiple logs
        this.logHandler.setTerm(1);
        for (long i = 1; i <= 5; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        startManager();
        Thread.sleep(500);

        // Connect a follower
        try (SocketChannel client = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);

            // Initial sync - follower gets all logs
            Utils.sendMessage(client, new RequestEntry(0, 0));
            Message resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp);
            AppendEntry initialResponse = (AppendEntry) resp;
            assertEquals(1, initialResponse.id());

            // Follower responds that it only applied up to log 2 (behind)
            Utils.sendMessage(client, new AppendEntryResponse(2, 1, true));
            // Controller should send catch-up entries (logs 3, 4, 5)
            resp = Utils.receiveMessage(client);
            assertInstanceOf(AppendEntry.class, resp, "Expected catch-up entries");

            final AppendEntry catchUpResponse = (AppendEntry) resp;
            assertEquals(3, catchUpResponse.id(), "Catch-up should start from log 3");
            assertFalse(catchUpResponse.entries().isEmpty(), "Expected catch-up commands");
        }
    }

    @Test
    @DisplayName("Multiple followers with mixed states")
    void multipleFollowers_mixedStates_handledCorrectly() throws Exception {
        // Setup - create logs
        this.logHandler.setTerm(1);
        for (long i = 1; i <= 3; i++) {
            this.kvStore.put("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        startManager();
        Thread.sleep(500);

        // Connect 3 followers
        try (SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
             SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
             SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress())) {
            this.managerNew.setRole(NodeRole.CONTROLLER);

            // Sync all followers
            Utils.sendMessage(client1, new RequestEntry(0, 0));
            Utils.sendMessage(client2, new RequestEntry(0, 0));
            Utils.sendMessage(client3, new RequestEntry(0, 0));

            Utils.receiveMessage(client1);
            Utils.receiveMessage(client2);
            Utils.receiveMessage(client3);

            // Follower 1 responds with current prevLogId (up to date)
            Utils.sendMessage(client1, new AppendEntryResponse(3, 1, true));
            Thread.sleep(50);

            // Follower 2 responds with older prevLogId (behind)
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));
            Thread.sleep(50);

            // Follower 2 should receive catch-up entries
            final Message resp2 = Utils.receiveMessage(client2);
            assertInstanceOf(AppendEntry.class, resp2, "Follower 2 should get catch-up");

            final AppendEntry catchUp = (AppendEntry) resp2;
            assertEquals(2, catchUp.id(), "Catch-up should start from log 2");

            // Follower 3 responds with current prevLogId
            Utils.sendMessage(client3, new AppendEntryResponse(3, 1, true));
            Thread.sleep(50);

            // Verify all sessions are properly tracked
            Utils.sendMessage(client1, new RequestEntry(3, 1));
            final Message resp1 = Utils.receiveMessage(client1);
            assertInstanceOf(AppendEntry.class, resp1);
        }
    }
}
