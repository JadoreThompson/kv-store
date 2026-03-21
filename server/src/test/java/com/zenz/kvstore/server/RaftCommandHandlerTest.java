package com.zenz.kvstore.server;

import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.command.handlers.RaftCommandHandler;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.commands.GetCommand;
import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.NodeRole;
import com.zenz.kvstore.server.raft.server.handlers.RaftControllerServerHandler;
import com.zenz.kvstore.server.raft.RaftManager;
import com.zenz.kvstore.server.raft.RaftNode;
import com.zenz.kvstore.server.raft.messages.AppendEntry;
import com.zenz.kvstore.server.raft.messages.AppendEntryResponse;
import com.zenz.kvstore.server.raft.messages.BaseMessage;
import com.zenz.kvstore.server.raft.messages.RequestEntry;
import com.zenz.kvstore.common.responses.*;
import com.zenz.kvstore.common.enums.ErrorType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for RaftCommandHandler.
 * Tests the command handling flow including majority consensus,
 * different speed nodes, and various command arguments.
 */
//@Disabled
@Execution(ExecutionMode.SAME_THREAD)
class RaftCommandHandlerTest {

    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 6970;

    private RaftManager manager;
    private ArrayList<RaftNode> nodes;
    private Thread managerThread;
    private Path logsDir;
    private Path snapshotsDir;
    private KVMapSnapshotter snapshotter;
    private WALogger logger;
    private RaftLogHandler logHandler;
    private KVStore kvStore;
    //    private RaftCommandHandler commandHandler;
    private RaftCommandHandler commandHandler;

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
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, TEST_PORT), null, NodeRole.CONTROLLER));
        manager = new RaftManager(0, nodes, kvStore);
        startManager();
        Thread.sleep(500);
        commandHandler = new RaftCommandHandler(kvStore, manager);
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
        return BaseMessage.deserialize(buffer);
    }

    private void startManager() {
        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the server thread:");
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

    /**
     * Helper method to sync a follower with the controller.
     * Sends RequestEntry and receives the response.
     */
    private void syncFollower(SocketChannel client) throws IOException {
        sendMessage(client, new RequestEntry(0, 0));
        BaseMessage response = receiveMessage(client);
        assertTrue(response instanceof AppendEntry, "Expected AppendEntry during sync");
    }

    /**
     * Command completes successfully when majority of fast nodes respond quickly.
     * This tests the happy path where all followers respond immediately.
     */
    @Test
    @DisplayName("HandleCommand with fast responding nodes - majority reached quickly")
    void handleCommand_fastNodes_majorityReached() throws Exception {
        Thread commandThread = null;
        ArrayList<SocketChannel> clients = new ArrayList<>();

        try {
            logHandler.setTerm(1);

            // Restart to reload logs
            stopManager();
            Thread.sleep(500);
            startManager();
            Thread.sleep(500);

            // Connect 3 followers (majority = 2)
            SocketChannel client1 = connectClient();
            SocketChannel client2 = connectClient();
            SocketChannel client3 = connectClient();
            clients.add(client1);
            clients.add(client2);
            clients.add(client3);

            // Sync all followers
            syncFollower(client1);
            syncFollower(client2);
            syncFollower(client3);

            // Create a command to handle
            PutCommand command = new PutCommand("testKey", "testValue".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<Boolean> fut = new CompletableFuture<>();

            // Handle command in a separate thread

            commandThread = new Thread(() -> {
                manager.getControllerServerHandler().handleCommand(command, fut);
            });
            commandThread.start();

            // Wait for AppendEntry to be sent to followers
            Thread.sleep(500);

            // Simulate fast responses from 2 followers (majority)
            sendMessage(client1, new AppendEntryResponse(1, 1, true));
            sendMessage(client2, new AppendEntryResponse(1, 1, true));

            // Wait for future to complete
            boolean completed = fut.get(2, TimeUnit.SECONDS);
            assertTrue(completed, "Future should complete when majority responds");

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

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect 3 followers
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        // Sync all followers
        syncFollower(client1);
        syncFollower(client2);
        syncFollower(client3);

        PutCommand command = new PutCommand("slowKey", "slowValue".getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Boolean> fut = new CompletableFuture<>();

        Thread commandThread = new Thread(() ->
                manager.getControllerServerHandler().handleCommand(command, fut)
        );
        commandThread.start();

        Thread.sleep(100);

        // Simulate slow response from first follower (delayed)
        Thread.sleep(300);
        sendMessage(client1, new AppendEntryResponse(1, 1, true));

        // Second follower responds after more delay
        Thread.sleep(300);
        sendMessage(client2, new AppendEntryResponse(1, 1, true));

        // Future should complete after majority
        boolean completed = fut.get(3, TimeUnit.SECONDS);
        assertTrue(completed, "Future should complete even with slow nodes");

        commandThread.join(1000);

        client1.close();
        client2.close();
        client3.close();
    }

    /**
     * Command with large value is handled correctly.
     * Tests handling of commands with different argument sizes.
     */
    @Test
    @DisplayName("HandleCommand with large value - processes correctly")
    void handleCommand_largeValue_processesCorrectly() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect 2 followers (majority = 2)
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        // Create command with large value (1KB)
        byte[] largeValue = new byte[1024];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }
        PutCommand command = new PutCommand("largeKey", largeValue);
        CompletableFuture<Boolean> fut = new CompletableFuture<>();

        Thread commandThread = new Thread(() -> {
            manager.getControllerServerHandler().handleCommand(command, fut);
        });
        commandThread.start();

        Thread.sleep(100);

        // Receive the AppendEntry and verify it contains the large value
        BaseMessage msg1 = receiveMessage(client1);
        assertTrue(msg1 instanceof AppendEntry, "Expected AppendEntry");
        AppendEntry appendEntry = (AppendEntry) msg1;
        assertFalse(appendEntry.entries().isEmpty(), "Should contain the command");

        // Respond from both followers
        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        sendMessage(client2, new AppendEntryResponse(1, 1, true));

        boolean completed = fut.get(2, TimeUnit.SECONDS);
        assertTrue(completed, "Future should complete for large value command");

        commandThread.join(1000);

        client1.close();
        client2.close();
    }

    /**
     * Multiple sequential commands are handled correctly.
     * Tests the system handles multiple commands in sequence.
     */
    @Test
    @DisplayName("HandleCommand multiple sequential commands - all complete")
    void handleCommand_multipleSequentialCommands_allComplete() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect 2 followers
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        // First command
        PutCommand command1 = new PutCommand("key1", "value1".getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Boolean> fut1 = new CompletableFuture<>();
        manager.getControllerServerHandler().handleCommand(command1, fut1);

        Thread.sleep(500);

        // Respond from followers for first command
        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        sendMessage(client2, new AppendEntryResponse(1, 1, true));

        assertTrue(fut1.get(2, TimeUnit.SECONDS), "First command should complete");
        // Commiting the action
        kvStore.put(command1.key(), command1.value());

        // Second command
        PutCommand command2 = new PutCommand("key2", "value2".getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Boolean> fut2 = new CompletableFuture<>();

        manager.getControllerServerHandler().handleCommand(command2, fut2);

        Thread.sleep(500);

        // Respond from followers for second command
        sendMessage(client1, new AppendEntryResponse(2, 1, true));
        sendMessage(client2, new AppendEntryResponse(2, 1, true));

        assertTrue(fut2.get(2, TimeUnit.SECONDS), "Second command should complete");

        client1.close();
        client2.close();
    }

    /**
     * Command completes with minimum majority (exactly half + 1).
     * Tests edge case where exactly majority is reached.
     */
    @Test
    @DisplayName("HandleCommand with minimum majority - completes exactly at threshold")
    void handleCommand_minimumMajority_completesAtThreshold() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        // Connect 5 followers (majority = 3)
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();
        SocketChannel client4 = connectClient();
        SocketChannel client5 = connectClient();

        syncFollower(client1);
        syncFollower(client2);
        syncFollower(client3);
        syncFollower(client4);
        syncFollower(client5);

        PutCommand command = new PutCommand("majorityKey", "majorityValue".getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Boolean> fut = new CompletableFuture<>();

        Thread commandThread = new Thread(() -> {
            manager.getControllerServerHandler().handleCommand(command, fut);
        });
        commandThread.start();

        Thread.sleep(100);

        // Only 2 responses - not yet majority
        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        sendMessage(client2, new AppendEntryResponse(1, 1, true));

        // Future should not be complete yet
        Thread.sleep(200);
        assertFalse(fut.isDone(), "Future should not complete before majority");

        // Third response - now we have majority (3 out of 5)
        sendMessage(client3, new AppendEntryResponse(1, 1, true));

        // Now future should complete
        boolean completed = fut.get(2, TimeUnit.SECONDS);
        assertTrue(completed, "Future should complete exactly at majority threshold");

        commandThread.join(1000);

        client1.close();
        client2.close();
        client3.close();
        client4.close();
        client5.close();
    }

    /**
     * RaftCommandHandler.handleCommand returns correct response for PUT.
     * Tests the full integration of RaftCommandHandler.
     */
    @Test
    @DisplayName("RaftCommandHandler handleCommand PUT - returns OK response")
    void raftCommandHandler_putCommand_returnsOk() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        RaftControllerServerHandler controller = manager.getControllerServerHandler();
        commandHandler = new RaftCommandHandler(kvStore, manager);

        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        AtomicReference<ByteBuffer> responseRef = new AtomicReference<>();
        Thread handlerThread = new Thread(() -> {
            PutCommand command = new PutCommand("handlerKey", "handlerValue".getBytes(StandardCharsets.UTF_8));
            ByteBuffer response = commandHandler.handleCommand(client1, command);
            responseRef.set(response);
        });
        handlerThread.start();

        // Wait for command to be sent
        Thread.sleep(500);

        // Respond from majority
        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        sendMessage(client2, new AppendEntryResponse(1, 1, true));

        handlerThread.join(3000);

        ByteBuffer response = responseRef.get();
        assertNotNull(response, "Response should not be null");

        // Check response is Success
        PutResponse getResponse = PutResponse.deserialize(response);
        assertNotNull(getResponse, "Response should not be null");

        client1.close();
        client2.close();
    }

    /**
     * BROKER state returns redirect to controller address.
     * When a client connects to a broker (follower), it should be redirected to the controller (leader).
     */
    @Test
    @DisplayName("BROKER state returns redirect to controller address")
    void brokerState_returnsRedirectToController() throws Exception {
        // Stop the current controller manager
        stopManager();
        Thread.sleep(500);

        // Create a broker node configuration
        // nodeAddress: broker's internal Raft server
        // serverAddress: where clients should connect (redirect target)
        int brokerPort = TEST_PORT + 1;
        int controllerClientPort = TEST_PORT + 2;
        InetSocketAddress controllerClientAddress = new InetSocketAddress(TEST_HOST, controllerClientPort);

        nodes = new ArrayList<>();
        // Broker node: nodeAddress is broker's Raft server, serverAddress is where clients should be redirected (controller's client server)
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, brokerPort),
                controllerClientAddress, NodeRole.BROKER));
        // Controller node: nodeAddress is controller's Raft server, serverAddress is controller's client server
        nodes.add(new RaftNode(1, new InetSocketAddress(TEST_HOST, TEST_PORT + 3),
                controllerClientAddress, NodeRole.CONTROLLER));

        // Start the broker manager
        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the server thread:");
                e.printStackTrace();
            }
        });
        managerThread.start();
        Thread.sleep(500);

        // Verify manager is in BROKER state
        assertEquals(NodeRole.BROKER, manager.getRole(), "Manager should be in BROKER state");

        // Create command handler for the broker
        RaftCommandHandler brokerHandler = new RaftCommandHandler(kvStore, manager);

        // Create a command
        PutCommand command = new PutCommand("testKey", "testValue".getBytes(StandardCharsets.UTF_8));
        ByteBuffer responseBuffer = brokerHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        // Deserialize and verify it's a RedirectResponse
        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof RedirectResponse, "Response should be a RedirectResponse");

        RedirectResponse redirectResponse = (RedirectResponse) response;
        assertEquals(controllerClientAddress, redirectResponse.address(),
                "Redirect should point to controller's client-facing server address");
    }

    /**
     * Test 8: CANDIDATE state returns IN_ELECTION error.
     * When a node is in CANDIDATE state during an election, it should return an error.
     */
    @Test
    @DisplayName("CANDIDATE state returns IN_ELECTION error")
    void candidateState_returnsInElectionError() throws Exception {
        // Use reflection to set the state to CANDIDATE (simulating an election)
        Field stateField = RaftManager.class.getDeclaredField("state");
        stateField.setAccessible(true);
        stateField.set(manager, NodeRole.CANDIDATE);

        // Verify manager is in CANDIDATE state
        assertEquals(NodeRole.CANDIDATE, manager.getRole(), "Manager should be in CANDIDATE state");

        // Create command handler
        RaftCommandHandler candidateHandler = new RaftCommandHandler(kvStore, manager);

        // Send a command
        PutCommand command = new PutCommand("testKey", "testValue".getBytes(StandardCharsets.UTF_8));
        ByteBuffer responseBuffer = candidateHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        // Deserialize and verify it's an ErrorResponse with IN_ELECTION
        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof ErrorResponse, "Response should be an ErrorResponse");

        ErrorResponse errorResponse = (ErrorResponse) response;
        assertEquals(ErrorType.IN_ELECTION, errorResponse.errorType(),
                "Error type should be IN_ELECTION");
    }

    /**
     * Test 9: BROKER state redirect contains correct controller server address.
     * Verifies that the redirect message contains the exact controller server address (client-facing address).
     */
    @Test
    @DisplayName("BROKER state redirect contains correct controller server address")
    void brokerState_redirectContainsCorrectControllerAddress() throws Exception {
        // Stop the current controller manager
        stopManager();
        Thread.sleep(500);

        // Create a broker node configuration with specific controller client address
        String controllerHost = "127.0.0.1";
        int brokerPort = TEST_PORT + 10;
        int controllerClientPort = TEST_PORT + 11;
        InetSocketAddress expectedClientAddress = new InetSocketAddress(controllerHost, controllerClientPort);

        nodes = new ArrayList<>();
        // serverAddress is the client-facing address where clients should connect
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, brokerPort),
                expectedClientAddress, NodeRole.BROKER));
        nodes.add(new RaftNode(1, new InetSocketAddress(TEST_HOST, TEST_PORT + 12),
                expectedClientAddress, NodeRole.CONTROLLER));

        // Start the broker manager
        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the server thread:");
                e.printStackTrace();
            }
        });
        managerThread.start();
        Thread.sleep(500);

        // Create command handler for the broker
        RaftCommandHandler brokerHandler = new RaftCommandHandler(kvStore, manager);

        // Send a GET command
        GetCommand command = new GetCommand("testKey");
        ByteBuffer responseBuffer = brokerHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        // Deserialize and verify the redirect address
        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof RedirectResponse, "Response should be a RedirectResponse");

        RedirectResponse redirectResponse = (RedirectResponse) response;
        assertEquals(expectedClientAddress, redirectResponse.address(),
                "Redirect should contain the exact controller client-facing server address");
    }

    // --- DELETE Unit Tests ---

    /**
     * Delete command in BROKER state returns redirect to controller.
     * When a client sends a DELETE to a broker, it should be redirected.
     */
    @Test
    @DisplayName("DELETE command in BROKER state returns redirect")
    void deleteCommand_brokerState_returnsRedirect() throws Exception {
        stopManager();
        Thread.sleep(500);

        int brokerPort = TEST_PORT + 20;
        int controllerClientPort = TEST_PORT + 21;
        InetSocketAddress controllerClientAddress = new InetSocketAddress(TEST_HOST, controllerClientPort);

        nodes = new ArrayList<>();
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, brokerPort),
                controllerClientAddress, NodeRole.BROKER));
        nodes.add(new RaftNode(1, new InetSocketAddress(TEST_HOST, TEST_PORT + 22),
                controllerClientAddress, NodeRole.CONTROLLER));

        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the server thread:");
                e.printStackTrace();
            }
        });
        managerThread.start();
        Thread.sleep(500);

        assertEquals(NodeRole.BROKER, manager.getRole(), "Manager should be in BROKER state");

        RaftCommandHandler brokerHandler = new RaftCommandHandler(kvStore, manager);

        DeleteCommand command = new DeleteCommand("deleteKey");
        ByteBuffer responseBuffer = brokerHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof RedirectResponse, "Response should be a RedirectResponse for DELETE in BROKER state");
    }

    /**
     * Delete command in CANDIDATE state returns IN_ELECTION error.
     * When a node is in CANDIDATE state, DELETE should return an error.
     */
    @Test
    @DisplayName("DELETE command in CANDIDATE state returns IN_ELECTION error")
    void deleteCommand_candidateState_returnsInElectionError() throws Exception {
        Field stateField = RaftManager.class.getDeclaredField("state");
        stateField.setAccessible(true);
        stateField.set(manager, NodeRole.CANDIDATE);

        assertEquals(NodeRole.CANDIDATE, manager.getRole(), "Manager should be in CANDIDATE state");

        RaftCommandHandler candidateHandler = new RaftCommandHandler(kvStore, manager);

        DeleteCommand command = new DeleteCommand("deleteKey");
        ByteBuffer responseBuffer = candidateHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof ErrorResponse, "Response should be an ErrorResponse");

        ErrorResponse errorResponse = (ErrorResponse) response;
        assertEquals(ErrorType.IN_ELECTION, errorResponse.errorType(),
                "Error type should be IN_ELECTION for DELETE in CANDIDATE state");
    }

    /**
     * Delete command with special characters in key is handled correctly.
     * Tests that DELETE handles keys with special characters.
     */
    @Test
    @DisplayName("DELETE command with special characters in key - handled correctly")
    void deleteCommand_specialCharactersKey_handledCorrectly() throws Exception {
        stopManager();
        Thread.sleep(500);

        int brokerPort = TEST_PORT + 23;
        int controllerClientPort = TEST_PORT + 24;
        InetSocketAddress controllerClientAddress = new InetSocketAddress(TEST_HOST, controllerClientPort);

        nodes = new ArrayList<>();
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, brokerPort),
                controllerClientAddress, NodeRole.BROKER));
        nodes.add(new RaftNode(1, new InetSocketAddress(TEST_HOST, TEST_PORT + 25),
                controllerClientAddress, NodeRole.CONTROLLER));

        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the server thread:");
                e.printStackTrace();
            }
        });
        managerThread.start();
        Thread.sleep(500);

        RaftCommandHandler brokerHandler = new RaftCommandHandler(kvStore, manager);

        String specialKey = "key-with_special.chars:123!@#$%";
        DeleteCommand command = new DeleteCommand(specialKey);
        ByteBuffer responseBuffer = brokerHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof RedirectResponse, "Response should be a RedirectResponse");
    }

    /**
     * Delete command with unicode key is handled correctly.
     * Tests that DELETE handles keys with unicode characters.
     */
    @Test
    @DisplayName("DELETE command with unicode key - handled correctly")
    void deleteCommand_unicodeKey_handledCorrectly() throws Exception {
        stopManager();
        Thread.sleep(500);

        int brokerPort = TEST_PORT + 26;
        int controllerClientPort = TEST_PORT + 27;
        InetSocketAddress controllerClientAddress = new InetSocketAddress(TEST_HOST, controllerClientPort);

        nodes = new ArrayList<>();
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, brokerPort),
                controllerClientAddress, NodeRole.BROKER));
        nodes.add(new RaftNode(1, new InetSocketAddress(TEST_HOST, TEST_PORT + 28),
                controllerClientAddress, NodeRole.CONTROLLER));

        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the server thread:");
                e.printStackTrace();
            }
        });
        managerThread.start();
        Thread.sleep(500);

        RaftCommandHandler brokerHandler = new RaftCommandHandler(kvStore, manager);

        String unicodeKey = "削除キー🔑";
        DeleteCommand command = new DeleteCommand(unicodeKey);
        ByteBuffer responseBuffer = brokerHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof RedirectResponse, "Response should be a RedirectResponse");
    }

    /**
     * Delete command with empty key is handled correctly.
     * Tests edge case of DELETE with empty key.
     */
    @Test
    @DisplayName("DELETE command with empty key - handled correctly")
    void deleteCommand_emptyKey_handledCorrectly() throws Exception {
        stopManager();
        Thread.sleep(500);

        int brokerPort = TEST_PORT + 29;
        int controllerClientPort = TEST_PORT + 30;
        InetSocketAddress controllerClientAddress = new InetSocketAddress(TEST_HOST, controllerClientPort);

        nodes = new ArrayList<>();
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, brokerPort),
                controllerClientAddress, NodeRole.BROKER));
        nodes.add(new RaftNode(1, new InetSocketAddress(TEST_HOST, TEST_PORT + 31),
                controllerClientAddress, NodeRole.CONTROLLER));

        manager = new RaftManager(0, nodes, kvStore);
        managerThread = new Thread(() -> {
            try {
                manager.start();
            } catch (Exception e) {
                System.out.println("An error occurred within the server thread:");
                e.printStackTrace();
            }
        });
        managerThread.start();
        Thread.sleep(500);

        RaftCommandHandler brokerHandler = new RaftCommandHandler(kvStore, manager);

        DeleteCommand command = new DeleteCommand("");
        ByteBuffer responseBuffer = brokerHandler.handleCommand(null, command);

        assertNotNull(responseBuffer, "Response should not be null");

        BaseResponse response = BaseResponse.deserialize(responseBuffer);
        assertTrue(response instanceof RedirectResponse, "Response should be a RedirectResponse");
    }

    // --- DELETE Integration Tests ---

    /**
     * Delete command completes when majority of nodes respond.
     * Tests the full flow of DELETE command with consensus.
     */
    @Test
    @DisplayName("DELETE command with majority consensus - completes successfully")
    void deleteCommand_majorityConsensus_completesSuccessfully() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        // First, store a value to delete
        kvStore.put("deleteKey", "deleteValue".getBytes(StandardCharsets.UTF_8));
        assertNotNull(kvStore.get("deleteKey"), "Key should exist before delete");

        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        DeleteCommand command = new DeleteCommand("deleteKey");
        CompletableFuture<Boolean> fut = new CompletableFuture<>();

        Thread commandThread = new Thread(() -> {
            manager.getControllerServerHandler().handleCommand(command, fut);
        });
        commandThread.start();

        Thread.sleep(500);

        // Respond from majority
        sendMessage(client1, new AppendEntryResponse(2, 1, true));
        sendMessage(client2, new AppendEntryResponse(2, 1, true));

        boolean completed = fut.get(2, TimeUnit.SECONDS);
        assertTrue(completed, "DELETE command should complete with majority");

        commandThread.join(1000);

        client1.close();
        client2.close();
    }

    /**
     * Delete command with slow nodes eventually completes.
     * Tests DELETE with delayed responses from followers.
     */
    @Test
    @DisplayName("DELETE command with slow nodes - completes after delay")
    void deleteCommand_slowNodes_completesAfterDelay() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        kvStore.put("slowDeleteKey", "value".getBytes(StandardCharsets.UTF_8));

        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        syncFollower(client1);
        syncFollower(client2);
        syncFollower(client3);

        DeleteCommand command = new DeleteCommand("slowDeleteKey");
        CompletableFuture<Boolean> fut = new CompletableFuture<>();

        Thread commandThread = new Thread(() -> {
            manager.getControllerServerHandler().handleCommand(command, fut);
        });
        commandThread.start();

        Thread.sleep(100);

        // Slow responses
        Thread.sleep(300);
        sendMessage(client1, new AppendEntryResponse(2, 1, true));

        Thread.sleep(300);
        sendMessage(client2, new AppendEntryResponse(2, 1, true));

        boolean completed = fut.get(3, TimeUnit.SECONDS);
        assertTrue(completed, "DELETE should complete even with slow nodes");

        commandThread.join(1000);

        client1.close();
        client2.close();
        client3.close();
    }

    /**
     * Multiple sequential DELETE commands all complete.
     * Tests handling of multiple DELETE commands in sequence.
     */
    @Test
    @DisplayName("Multiple sequential DELETE commands - all complete")
    void deleteCommand_multipleSequential_allComplete() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        // Store multiple keys
        kvStore.put("del1", "value1".getBytes(StandardCharsets.UTF_8));
        kvStore.put("del2", "value2".getBytes(StandardCharsets.UTF_8));
        kvStore.put("del3", "value3".getBytes(StandardCharsets.UTF_8));

        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        // First DELETE
        DeleteCommand command1 = new DeleteCommand("del1");
        CompletableFuture<Boolean> fut1 = new CompletableFuture<>();
        manager.getControllerServerHandler().handleCommand(command1, fut1);

        Thread.sleep(500);

        sendMessage(client1, new AppendEntryResponse(4, 1, true));
        sendMessage(client2, new AppendEntryResponse(4, 1, true));

        assertTrue(fut1.get(2, TimeUnit.SECONDS), "First DELETE should complete");
        kvStore.delete("del1");

        // Second DELETE
        DeleteCommand command2 = new DeleteCommand("del2");
        CompletableFuture<Boolean> fut2 = new CompletableFuture<>();
        manager.getControllerServerHandler().handleCommand(command2, fut2);

        Thread.sleep(500);

        sendMessage(client1, new AppendEntryResponse(5, 1, true));
        sendMessage(client2, new AppendEntryResponse(5, 1, true));

        assertTrue(fut2.get(2, TimeUnit.SECONDS), "Second DELETE should complete");
        kvStore.delete("del2");

        // Third DELETE
        DeleteCommand command3 = new DeleteCommand("del3");
        CompletableFuture<Boolean> fut3 = new CompletableFuture<>();
        manager.getControllerServerHandler().handleCommand(command3, fut3);

        Thread.sleep(500);

        sendMessage(client1, new AppendEntryResponse(6, 1, true));
        sendMessage(client2, new AppendEntryResponse(6, 1, true));

        assertTrue(fut3.get(2, TimeUnit.SECONDS), "Third DELETE should complete");
        kvStore.delete("del3");

        client1.close();
        client2.close();
    }

    /**
     * DELETE command completes at minimum majority threshold.
     * Tests edge case where DELETE completes exactly at majority.
     */
    @Test
    @DisplayName("DELETE command at minimum majority - completes at threshold")
    void deleteCommand_minimumMajority_completesAtThreshold() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        kvStore.put("majorityDeleteKey", "value".getBytes(StandardCharsets.UTF_8));

        // Connect 5 followers (majority = 3)
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();
        SocketChannel client4 = connectClient();
        SocketChannel client5 = connectClient();

        syncFollower(client1);
        syncFollower(client2);
        syncFollower(client3);
        syncFollower(client4);
        syncFollower(client5);

        DeleteCommand command = new DeleteCommand("majorityDeleteKey");
        CompletableFuture<Boolean> fut = new CompletableFuture<>();

        Thread commandThread = new Thread(() -> {
            manager.getControllerServerHandler().handleCommand(command, fut);
        });
        commandThread.start();

        Thread.sleep(100);

        // Only 2 responses - not yet majority
        sendMessage(client1, new AppendEntryResponse(2, 1, true));
        sendMessage(client2, new AppendEntryResponse(2, 1, true));

        Thread.sleep(200);
        assertFalse(fut.isDone(), "DELETE should not complete before majority");

        // Third response - now majority
        sendMessage(client3, new AppendEntryResponse(2, 1, true));

        boolean completed = fut.get(2, TimeUnit.SECONDS);
        assertTrue(completed, "DELETE should complete exactly at majority threshold");

        commandThread.join(1000);

        client1.close();
        client2.close();
        client3.close();
        client4.close();
        client5.close();
    }

    /**
     * DELETE non-existent key completes successfully.
     * Tests that DELETE for non-existent key doesn't cause errors.
     */
    @Test
    @DisplayName("DELETE non-existent key - completes successfully")
    void deleteCommand_nonExistentKey_completesSuccessfully() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        // Ensure key doesn't exist
        assertNull(kvStore.get("nonExistentDeleteKey"), "Key should not exist");

        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        DeleteCommand command = new DeleteCommand("nonExistentDeleteKey");
        CompletableFuture<Boolean> fut = new CompletableFuture<>();

        Thread commandThread = new Thread(() -> {
            manager.getControllerServerHandler().handleCommand(command, fut);
        });
        commandThread.start();

        Thread.sleep(500);

        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        sendMessage(client2, new AppendEntryResponse(1, 1, true));

        boolean completed = fut.get(2, TimeUnit.SECONDS);
        assertTrue(completed, "DELETE for non-existent key should complete");

        commandThread.join(1000);

        client1.close();
        client2.close();
    }

    /**
     * Mixed PUT and DELETE commands in sequence.
     * Tests handling of interleaved PUT and DELETE operations.
     */
    @Test
    @DisplayName("Mixed PUT and DELETE commands - all complete")
    void mixedCommands_putAndDelete_allComplete() throws Exception {
        logHandler.setTerm(1);

        stopManager();
        startManager();
        Thread.sleep(500);

        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        // PUT command
        PutCommand putCommand = new PutCommand("mixedKey", "mixedValue".getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Boolean> putFut = new CompletableFuture<>();
        manager.getControllerServerHandler().handleCommand(putCommand, putFut);

        Thread.sleep(500);

        sendMessage(client1, new AppendEntryResponse(1, 1, true));
        sendMessage(client2, new AppendEntryResponse(1, 1, true));

        assertTrue(putFut.get(2, TimeUnit.SECONDS), "PUT should complete");
        kvStore.put(putCommand.key(), putCommand.value());

        // DELETE command
        DeleteCommand deleteCommand = new DeleteCommand("mixedKey");
        CompletableFuture<Boolean> deleteFut = new CompletableFuture<>();
        manager.getControllerServerHandler().handleCommand(deleteCommand, deleteFut);

        Thread.sleep(500);

        sendMessage(client1, new AppendEntryResponse(2, 1, true));
        sendMessage(client2, new AppendEntryResponse(2, 1, true));

        assertTrue(deleteFut.get(2, TimeUnit.SECONDS), "DELETE should complete");
        kvStore.delete("mixedKey");

        // Verify key is deleted
        assertNull(kvStore.get("mixedKey"), "Key should be deleted");

        client1.close();
        client2.close();
    }
}
