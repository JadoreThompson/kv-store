package main.java.com.zenz.kvstore;

import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.WALogger;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.raft.NodeState;
import com.zenz.kvstore.raft.RaftControllerServerHandler;
import com.zenz.kvstore.raft.RaftManager;
import com.zenz.kvstore.raft.RaftNode;
import com.zenz.kvstore.raft.messages.AppendEntry;
import com.zenz.kvstore.raft.messages.AppendEntryResponse;
import com.zenz.kvstore.raft.messages.BaseMessage;
import com.zenz.kvstore.raft.messages.RequestEntry;
import com.zenz.kvstore.commandHandlers.RaftCommandHandler;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.Array;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for RaftCommandHandler.
 * Tests the command handling flow including majority consensus,
 * different speed nodes, and various command arguments.
 */
//@Disabled
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
        nodes.add(new RaftNode(0, new InetSocketAddress(TEST_HOST, TEST_PORT), NodeState.CONTROLLER));
        manager = new RaftManager(0, nodes, kvStore);
        startManager();
        Thread.sleep(500);

        RaftControllerServerHandler controller = manager.getControllerServerHandler();
        commandHandler = new RaftCommandHandler(kvStore, controller);
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
     * Test 1: Command completes successfully when majority of fast nodes respond quickly.
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
     * Test 2: Command completes when slow nodes eventually respond.
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

        Thread commandThread = new Thread(() -> {
            manager.getControllerServerHandler().handleCommand(command, fut);
        });
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
     * Test 3: Command with large value is handled correctly.
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
     * Test 4: Multiple sequential commands are handled correctly.
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
     * Test 5: Command completes with minimum majority (exactly half + 1).
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
     * Test 6: RaftCommandHandler.handleCommand returns correct response for PUT.
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
        commandHandler = new RaftCommandHandler(kvStore, controller);

        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();

        syncFollower(client1);
        syncFollower(client2);

        AtomicReference<ByteBuffer> responseRef = new AtomicReference<>();
        Thread handlerThread = new Thread(() -> {
            PutCommand command = new PutCommand("handlerKey", "handlerValue".getBytes(StandardCharsets.UTF_8));
            ByteBuffer response = commandHandler.handleCommand(command);
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

        // Check response is "OK"
        byte[] responseBytes = new byte[response.remaining()];
        response.get(responseBytes);
        String responseStr = new String(responseBytes, StandardCharsets.UTF_8);
        assertTrue(responseStr.startsWith("OK"), "Response should start with OK");

        client1.close();
        client2.close();
    }
}