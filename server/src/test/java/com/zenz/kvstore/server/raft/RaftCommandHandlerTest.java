package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.responses.BaseResponse;
import com.zenz.kvstore.common.responses.ErrorResponse;
import com.zenz.kvstore.common.responses.PutResponse;
import com.zenz.kvstore.common.responses.RedirectResponse;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.command.handler.RaftCommandHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.AppendEntry;
import com.zenz.kvstore.server.raft.message.AppendEntryResponse;
import com.zenz.kvstore.server.raft.message.Message;
import com.zenz.kvstore.server.raft.message.RequestEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for RaftCommandHandler.
 * Tests the command handling flow including majority consensus,
 * different speed nodes, and various command arguments.
 */

@ExtendWith(MockitoExtension.class)
class RaftCommandHandlerTest {

    private static final Random random = new Random();

    private ExecutorService executorService;

    private Thread managerNewThread;

    private Path logsDir;

    private Path snapshotsDir;

    private RaftLogHandler logHandler;

    private KVStore kvStore;

    private RaftCommandHandler commandHandler;

    private NodeConfig nodeConfig;

    private Manager manager;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        this.executorService = Executors.newCachedThreadPool();
        this.logsDir = Files.createTempDirectory("logs-");
        this.snapshotsDir = Files.createTempDirectory("snapshots-");

        final KVMapSnapshotter snapshotter = new KVMapSnapshotter(snapshotsDir);
        final WALogger logger = new WALogger(logsDir.resolve("0.log"));
        this.logHandler = new RaftLogHandler(logger);
        this.kvStore = new KVStore(new KVStore.Builder()
                .setLogHandler(logHandler)
                .setSnapshotter(snapshotter)
        );

        this.nodeConfig = new NodeConfig(
                "origin",
                new InetSocketAddress("localhost", random.nextInt(1000, 9000)),
                null);
        Thread.sleep(500);
        this.commandHandler = new RaftCommandHandler(kvStore, this.manager);
    }

    @AfterEach
    void afterEach() throws IOException, InterruptedException {
        logsDir.toFile().delete();
        snapshotsDir.toFile().delete();
        stopManager();
    }

    private void startManager() {
        this.manager = new Manager(kvStore, nodeConfig, Collections.emptyList());
        this.managerNewThread = new Thread(
                () -> com.zenz.kvstore.common.utils.Utils.checkedRunnableWrapper(this.manager::start));
        this.managerNewThread.start();
        this.manager.setRole(NodeRole.CONTROLLER);
    }

    private void stopManager() throws InterruptedException, IOException {
        if (this.manager != null) {
            this.manager.stop();
        }

        if (this.managerNewThread != null) {
            this.managerNewThread.interrupt();
            this.managerNewThread.join(5000);
        }
    }

    /**
     * Helper method to sync a follower with the controller.
     * Sends RequestEntry and receives the response.
     */
    private void syncFollower(SocketChannel client) throws IOException {
        Utils.sendMessage(client, new RequestEntry(0, 0));
        Message response = Utils.receiveMessage(client);
        assertInstanceOf(AppendEntry.class, response, "Expected AppendEntry during sync");
    }

    /**
     * Command completes successfully when the majority of fast nodes respond quickly.
     * This tests the happy path where all followers respond immediately.
     */
    @Test
    @DisplayName("HandleCommand with fast responding nodes - majority reached quickly")
    void handleCommand_fastNodes_majorityReached() throws Exception {
        Thread commandThread = null;
        ArrayList<SocketChannel> clients = new ArrayList<>();

        try {
            logHandler.setTerm(1);
            startManager();
            this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
            Thread.sleep(500);

            // Connect 3 followers (majority = 2)
            SocketChannel client1 = Utils.connectClient(nodeConfig.serverAddress());
            SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
            SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress());
            clients.add(client1);
            clients.add(client2);
            clients.add(client3);

            // Sync all followers
            syncFollower(client1);
            syncFollower(client2);
            syncFollower(client3);

            // Create a command to handle
            final PutCommand command = new PutCommand("testKey", "testValue".getBytes(StandardCharsets.UTF_8));
            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));
            Thread.sleep(500);

            // Simulate fast responses from 2 followers (majority)
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            // Wait for future to complete
            fut.get(2, TimeUnit.SECONDS);
            assertTrue(fut.isDone(), "Future should complete when majority responds");

        } finally {
            if (commandThread != null) commandThread.join(1000);
            for (SocketChannel client : clients) client.close();
        }
    }

    /**
     * Command completes when slow nodes eventually respond.
     * Tests that the system handles nodes with different response times.
     */
    @Test
    @DisplayName("HandleCommand with slow nodes - majority reached after delay")
    void handleCommand_slowNodes_majorityReachedAfterDelay() throws Exception {
        logHandler.setTerm(1);

        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        // Connect 3 followers
        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            // Sync all followers
            syncFollower(client1);
            syncFollower(client2);
            syncFollower(client3);

            PutCommand command = new PutCommand("slowKey", "slowValue".getBytes(StandardCharsets.UTF_8));
            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));
            Thread.sleep(100);

            // Simulate slow response from first follower (delayed)
            Thread.sleep(300);
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));

            // Second follower responds after more delay
            Thread.sleep(300);
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            // Future should complete after majority
            fut.get(3, TimeUnit.SECONDS);
            assertTrue(fut.isDone(), "Future should complete even with slow nodes");
        } finally {
            client1.close();
            client2.close();
            client3.close();
        }
    }

    /**
     * Command with large value is handled correctly.
     * Tests handling of commands with different argument sizes.
     */
    @Test
    @DisplayName("HandleCommand with large value - processes correctly")
    void handleCommand_largeValue_processesCorrectly() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        // Connect 2 followers (majority = 2)
        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);

            // Create command with large value (1KB)
            byte[] largeValue = new byte[1024];
            for (int i = 0; i < largeValue.length; i++) {
                largeValue[i] = (byte) (i % 256);
            }
            PutCommand command = new PutCommand("largeKey", largeValue);

            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));

            Thread.sleep(500);

            // Respond from both followers
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            ByteBuffer result = fut.get(2, TimeUnit.SECONDS);
            assertNotNull(result, "Response should not be null for large value command");
            assertTrue(fut.isDone(), "Future should complete for large value command");
        } finally {
            client1.close();
            client2.close();
        }
    }

    /**
     * Multiple sequential commands are handled correctly.
     * Tests the system handles multiple commands in sequence.
     */
    @Test
    @DisplayName("HandleCommand multiple sequential commands - all complete")
    void handleCommand_multipleSequentialCommands_allComplete() throws Exception {
        logHandler.setTerm(1);

        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        // Connect 2 followers
        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);

            // First command
            PutCommand command1 = new PutCommand("key1", "value1".getBytes(StandardCharsets.UTF_8));
            Future<ByteBuffer> fut1 = this.executorService.submit(() -> this.commandHandler.handleCommand(command1));
            Thread.sleep(500);

            // Respond from followers for first command
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            ByteBuffer result1 = fut1.get(2, TimeUnit.SECONDS);
            assertNotNull(result1, "First command should return a response");

            // Second command
            PutCommand command2 = new PutCommand("key2", "value2".getBytes(StandardCharsets.UTF_8));
            Future<ByteBuffer> fut2 = this.executorService.submit(() -> this.commandHandler.handleCommand(command2));
            Thread.sleep(500);

            // Respond from followers for second command
            Utils.sendMessage(client1, new AppendEntryResponse(2, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(2, 1, true));

            ByteBuffer result2 = fut2.get(2, TimeUnit.SECONDS);
            assertNotNull(result2, "Second command should return a response");
        } finally {
            client1.close();
            client2.close();
        }
    }

    /**
     * Command completes with minimum majority (exactly half + 1).
     * Tests edge case where exactly majority is reached.
     */
    @Test
    @DisplayName("HandleCommand with minimum majority - completes exactly at threshold")
    void handleCommand_minimumMajority_completesAtThreshold() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        // Connect 5 followers (majority = 3)
        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client4 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client5 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);
            syncFollower(client3);
            syncFollower(client4);
            syncFollower(client5);

            PutCommand command = new PutCommand("majorityKey", "majorityValue".getBytes(StandardCharsets.UTF_8));
            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));

            Thread.sleep(500);

            // Only 2 responses - not yet majority
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            // Future should not be complete yet
            Thread.sleep(200);
            assertFalse(fut.isDone(), "Future should not complete before majority");

            // Third response - now we have majority (3 out of 5)
            Utils.sendMessage(client3, new AppendEntryResponse(1, 1, true));

            // Now future should complete
            ByteBuffer result = fut.get(2, TimeUnit.SECONDS);
            assertNotNull(result, "Future should complete exactly at majority threshold");
        } finally {
            client1.close();
            client2.close();
            client3.close();
            client4.close();
            client5.close();
        }
    }

    /**
     * RaftCommandHandler.handleCommand returns correct response for PUT.
     * Tests the full integration of RaftCommandHandler.
     */
    @Test
    @DisplayName("RaftCommandHandler handleCommand PUT - returns OK response")
    void raftCommandHandler_putCommand_returnsOk() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);

            Future<ByteBuffer> fut = this.executorService.submit(() -> {
                PutCommand command = new PutCommand("handlerKey", "handlerValue".getBytes(StandardCharsets.UTF_8));
                return commandHandler.handleCommand(command);
            });
            Thread.sleep(500);

            // Respond from majority
            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            ByteBuffer response = fut.get(3, TimeUnit.SECONDS);
            assertNotNull(response, "Response should not be null");

            // Check response is Success
            PutResponse putResponse = PutResponse.deserialize(response);
            assertNotNull(putResponse, "Response should not be null");
        } finally {
            client1.close();
            client2.close();
        }
    }

    @Test
    @DisplayName("CANDIDATE state returns IN_ELECTION error")
    void candidateState_returnsInElectionError() throws Exception {
        // Create a manager and set it to CANDIDATE state
        final int basePort = this.nodeConfig.serverAddress().getPort();
        NodeConfig candidateConfig = new NodeConfig("candidate", new InetSocketAddress("localhost", basePort + 40), null);
        this.manager = new Manager(kvStore, candidateConfig, Collections.emptyList());
        this.manager.setRole(NodeRole.CANDIDATE);

        // Verify manager is in CANDIDATE state
        assertEquals(NodeRole.CANDIDATE, manager.getRole(), "Manager should be in CANDIDATE state");

        // Create command handler
        RaftCommandHandler candidateHandler = new RaftCommandHandler(kvStore, manager);

        // Send a command
        PutCommand command = new PutCommand("testKey", "testValue".getBytes(StandardCharsets.UTF_8));
        ByteBuffer responseBuffer = candidateHandler.handleCommand(command);
        assertNotNull(responseBuffer, "Response should not be null");

        // Deserialize and verify it's an ErrorResponse with IN_ELECTION
        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof ErrorResponse, "Response should be an ErrorResponse");

        ErrorResponse errorResponse = (ErrorResponse) response;
        assertEquals(ErrorType.IN_ELECTION, errorResponse.errorType(),
                "Error type should be IN_ELECTION");
    }

    /**
     * BROKER state returns redirect to controller serverAddress.
     * When a client connects to a broker (follower), it should be redirected to the controller (leader).
     */
    @Test
    @DisplayName("BROKER state returns redirect to controller serverAddress")
    void brokerState_returnsRedirectToController() throws Exception {
        // Create a broker node configuration
        final int basePort = this.nodeConfig.serverAddress().getPort();
        int brokerPort = basePort + 1;
        InetSocketAddress controllerClientAddress = new InetSocketAddress("localhost", basePort + 2);

        // Create broker node config
        NodeConfig brokerConfig = new NodeConfig("broker", new InetSocketAddress("localhost", brokerPort), null);
        NodeConfig controllerConfig =
                new NodeConfig("controller", new InetSocketAddress("localhost", basePort + 3), controllerClientAddress);
        TestControllerServer testControllerServer =
                new TestControllerServer(controllerConfig.serverAddress().getPort());
        this.executorService.submit(() -> {
            try {
                testControllerServer.start();
            } catch (IOException e) {
            }
        });

        Thread.sleep(500);

        // Start the broker manager
        this.manager = new Manager(kvStore, brokerConfig, List.of(controllerConfig));
        this.managerNewThread = new Thread(() -> com.zenz.kvstore.common.utils.Utils.checkedRunnableWrapper(this.manager::start));
        this.managerNewThread.start();
        Thread.sleep(10_000);

        // Verify manager is in BROKER state
        assertEquals(NodeRole.BROKER, manager.getRole(), "Manager should be in BROKER state");

        // Create command handler for the broker
        RaftCommandHandler brokerHandler = new RaftCommandHandler(kvStore, manager);
        PutCommand command = new PutCommand("testKey", "testValue".getBytes(StandardCharsets.UTF_8));
        ByteBuffer responseBuffer = brokerHandler.handleCommand(command);

        assertNotNull(responseBuffer, "Response should not be null");

        // Deserialize and verify it's a RedirectResponse
        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertInstanceOf(RedirectResponse.class, response, "Response should be a RedirectResponse. Received:" + response);

        RedirectResponse redirectResponse = (RedirectResponse) response;
        assertEquals(controllerClientAddress, redirectResponse.address(),
                "Redirect should point to controller's client-facing server serverAddress");
    }

    // --- DELETE Integration Tests ---

    /**
     * Delete command completes when the majority of nodes respond.
     * Tests the full flow of DELETE command with consensus.
     */
    @Test
    @DisplayName("DELETE command with majority consensus - completes successfully")
    void deleteCommand_majorityConsensus_completesSuccessfully() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        // First, store a value to delete
        this.kvStore.put("deleteKey", "deleteValue".getBytes(StandardCharsets.UTF_8));
        assertNotNull(this.kvStore.get("deleteKey"), "Key should exist before delete");

        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);

            DeleteCommand command = new DeleteCommand("deleteKey");
            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));

            Thread.sleep(500);

            // Respond from majority
            final RaftLogHandler logHandler = (RaftLogHandler) this.kvStore.getLogHandler();
            final long term = logHandler.getTerm();
            final long nextLogId = logHandler.getLogId() + 1;
            Utils.sendMessage(client1, new AppendEntryResponse(nextLogId, term, true));
            Utils.sendMessage(client2, new AppendEntryResponse(nextLogId, term, true));

            ByteBuffer result = fut.get(2, TimeUnit.SECONDS);
            assertNotNull(result, "DELETE command should complete with majority");
        } finally {
            client1.close();
            client2.close();
        }
    }

    /**
     * Delete command with slow nodes eventually completes.
     * Tests DELETE with delayed responses from followers.
     */
    @Test
    @DisplayName("DELETE command with slow nodes - completes after delay")
    void deleteCommand_slowNodes_completesAfterDelay() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        this.kvStore.put("slowDeleteKey", "value".getBytes(StandardCharsets.UTF_8));

        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);
            syncFollower(client3);

            DeleteCommand command = new DeleteCommand("slowDeleteKey");
            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));

            // Slow responses
            Thread.sleep(300);
            final RaftLogHandler logHandler = (RaftLogHandler) this.kvStore.getLogHandler();
            final long term = logHandler.getTerm();
            final long nextLogId = logHandler.getLogId() + 1;
            Utils.sendMessage(client1, new AppendEntryResponse(nextLogId, term, true));

            Thread.sleep(300);
            Utils.sendMessage(client2, new AppendEntryResponse(nextLogId, term, true));

            ByteBuffer result = fut.get(3, TimeUnit.SECONDS);
            assertNotNull(result, "DELETE should complete even with slow nodes");
        } finally {
            client1.close();
            client2.close();
            client3.close();
        }
    }

    /**
     * Multiple sequential DELETE commands all complete.
     * Tests handling of multiple DELETE commands in sequence.
     */
    @Test
    @DisplayName("Multiple sequential DELETE commands - all complete")
    void deleteCommand_multipleSequential_allComplete() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        // Store multiple keys
        this.kvStore.put("del1", "value1".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("del2", "value2".getBytes(StandardCharsets.UTF_8));
        this.kvStore.put("del3", "value3".getBytes(StandardCharsets.UTF_8));

        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);

            // First DELETE
            DeleteCommand command1 = new DeleteCommand("del1");
            Future<ByteBuffer> fut1 = this.executorService.submit(() -> this.commandHandler.handleCommand(command1));

            Thread.sleep(500);

            final RaftLogHandler logHandler = (RaftLogHandler) this.kvStore.getLogHandler();
            final long term = logHandler.getTerm();
            long nextLogId = logHandler.getLogId() + 1;
            Utils.sendMessage(client1, new AppendEntryResponse(nextLogId, term, true));
            Utils.sendMessage(client2, new AppendEntryResponse(nextLogId, term, true));

            ByteBuffer result1 = fut1.get(2, TimeUnit.SECONDS);
            assertNotNull(result1, "First DELETE should complete");

            // Second DELETE
            DeleteCommand command2 = new DeleteCommand("del2");
            Future<ByteBuffer> fut2 = this.executorService.submit(() -> this.commandHandler.handleCommand(command2));
            nextLogId++;
            Thread.sleep(500);

            Utils.sendMessage(client1, new AppendEntryResponse(nextLogId, term, true));
            Utils.sendMessage(client2, new AppendEntryResponse(nextLogId, term, true));

            ByteBuffer result2 = fut2.get(2, TimeUnit.SECONDS);
            assertNotNull(result2, "Second DELETE should complete");

            // Third DELETE
            DeleteCommand command3 = new DeleteCommand("del3");
            Future<ByteBuffer> fut3 = this.executorService.submit(() -> this.commandHandler.handleCommand(command3));
            nextLogId++;

            Thread.sleep(500);

            Utils.sendMessage(client1, new AppendEntryResponse(nextLogId, term, true));
            Utils.sendMessage(client2, new AppendEntryResponse(nextLogId, term, true));

            ByteBuffer result3 = fut3.get(2, TimeUnit.SECONDS);
            assertNotNull(result3, "Third DELETE should complete");
        } finally {
            client1.close();
            client2.close();
        }
    }

    /**
     * DELETE command completes at minimum majority threshold.
     * Tests edge case where DELETE completes exactly at majority.
     */
    @Test
    @DisplayName("DELETE command at minimum majority - completes at threshold")
    void deleteCommand_minimumMajority_completesAtThreshold() throws Exception {
        logHandler.setTerm(1);

        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        this.kvStore.put("majorityDeleteKey", "value".getBytes(StandardCharsets.UTF_8));

        // Connect 5 followers (majority = 3)
        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client3 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client4 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client5 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);
            syncFollower(client3);
            syncFollower(client4);
            syncFollower(client5);

            DeleteCommand command = new DeleteCommand("majorityDeleteKey");
            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));

            Thread.sleep(500);

            // Only 2 responses - not yet majority
            final long nextLogId = this.kvStore.getLogHandler().getLogId() + 1;
            final long term = ((RaftLogHandler) this.kvStore.getLogHandler()).getTerm();
            Utils.sendMessage(client1, new AppendEntryResponse(nextLogId, term, true));
            Utils.sendMessage(client2, new AppendEntryResponse(nextLogId, term, true));

            Thread.sleep(200);
            assertFalse(fut.isDone(), "DELETE should not complete before majority");

            // Third response - now majority
            Utils.sendMessage(client3, new AppendEntryResponse(nextLogId, term, true));

            ByteBuffer result = fut.get(2, TimeUnit.SECONDS);
            assertNotNull(result, "DELETE should complete exactly at majority threshold");
        } finally {
            client1.close();
            client2.close();
            client3.close();
            client4.close();
            client5.close();
        }
    }

    /**
     * DELETE non-existent key completes successfully.
     * Tests that DELETE for non-existent key doesn't cause errors.
     */
    @Test
    @DisplayName("DELETE non-existent key - completes successfully")
    void deleteCommand_nonExistentKey_completesSuccessfully() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        // Ensure key doesn't exist
        assertNull(this.kvStore.get("nonExistentDeleteKey"), "Key should not exist");

        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);

            DeleteCommand command = new DeleteCommand("nonExistentDeleteKey");
            Future<ByteBuffer> fut = this.executorService.submit(() -> this.commandHandler.handleCommand(command));

            Thread.sleep(500);

            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            ByteBuffer result = fut.get(2, TimeUnit.SECONDS);
            assertNotNull(result, "DELETE for non-existent key should complete");
        } finally {
            client1.close();
            client2.close();
        }
    }

    /**
     * Mixed PUT and DELETE commands in sequence.
     * Tests handling of interleaved PUT and DELETE operations.
     */
    @Test
    @DisplayName("Mixed PUT and DELETE commands - all complete")
    void mixedCommands_putAndDelete_allComplete() throws Exception {
        logHandler.setTerm(1);
        startManager();
        this.commandHandler = new RaftCommandHandler(this.kvStore, this.manager);
        Thread.sleep(500);

        SocketChannel client1 = Utils.connectClient(this.nodeConfig.serverAddress());
        SocketChannel client2 = Utils.connectClient(this.nodeConfig.serverAddress());

        try {
            syncFollower(client1);
            syncFollower(client2);

            // PUT command
            PutCommand putCommand = new PutCommand("mixedKey", "mixedValue".getBytes(StandardCharsets.UTF_8));
            Future<ByteBuffer> putFut = this.executorService.submit(() -> this.commandHandler.handleCommand(putCommand));
            Thread.sleep(500);

            Utils.sendMessage(client1, new AppendEntryResponse(1, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(1, 1, true));

            ByteBuffer putResult = putFut.get(2, TimeUnit.SECONDS);
            assertNotNull(putResult, "PUT should complete");

            // DELETE command
            DeleteCommand deleteCommand = new DeleteCommand("mixedKey");
            Future<ByteBuffer> deleteFut = this.executorService.submit(() -> this.commandHandler.handleCommand(deleteCommand));
            Thread.sleep(500);

            Utils.sendMessage(client1, new AppendEntryResponse(2, 1, true));
            Utils.sendMessage(client2, new AppendEntryResponse(2, 1, true));

            ByteBuffer deleteResult = deleteFut.get(2, TimeUnit.SECONDS);
            assertNotNull(deleteResult, "DELETE should complete");

            // Verify key is deleted
            assertNull(this.kvStore.get("mixedKey"), "Key should be deleted");
        } finally {
            client1.close();
            client2.close();
        }
    }
}
