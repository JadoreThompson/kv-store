package main.java.com.zenz.kvstore.server;

import com.zenz.kvstore.server.*;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.*;
import com.zenz.kvstore.server.raft.messages.*;
import com.zenz.kvstore.common.utils.Utils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.SAME_THREAD)
class LeaderElectionTest {

    private Path tempDir;
    private Path logsFolderPath;
    private Path snapshotFolderPath;
    private final String DEBUG_PREFIX = "[LeaderElectionTest]";

    @BeforeEach
    void beforeEach() throws IOException {
        tempDir = Files.createTempDirectory("leader-election-test-");
        logsFolderPath = tempDir.resolve("logs");
        snapshotFolderPath = tempDir.resolve("snapshots");

        Files.createDirectories(logsFolderPath);
        Files.createDirectories(snapshotFolderPath);
    }

    @AfterEach
    void afterEach() {
        tempDir.toFile().delete();
    }

    /**
     * Creates a test KVStore with log handler and snapshotter.
     */
    private KVStore createStore(
            RaftLogHandler logHandler,
            KVMapSnapshotter snapshotter
    ) throws IOException {
        return new KVStore(
                new KVStore.Builder()
                        .setLogHandler(logHandler)
                        .setSnapshotter(snapshotter)
                        .setRaftMode(true)
        );
    }

    private void wrapper(CheckedRunnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            System.out.println(String.format("Exception in Thread '%s' ", Thread.currentThread().getName()));
            e.printStackTrace();
        }
    }

    /**
     * Tests the full election cycle:
     * 1. Election is initiated after controller failure
     * 2. Vote request is sent to broker
     * 3. Broker grants vote based on term
     * 4. Manager receives and processes vote response
     * 5. Election completes when majority is reached
     */
    @Test
    @DisplayName("Complete election cycle from initiation to completion")
    void completeElectionCycle_fromInitiationToCompletion_with3Nodes() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[completeElectionCycle_fromInitiationToCompletion_with3Nodes] ";

        ExecutorService executor = Executors.newFixedThreadPool(4);
        ArrayList<RaftManager> managers = new ArrayList<>();

        TestControllerServer controllerServer = new TestControllerServer(8999);

        try {
            // Starting controller server so brokers have something to connect to
            executor.execute(() -> Utils.runnableWrapper(controllerServer::start));

            // Wait for sever startup
            Thread.sleep(500);

            int attempts = 0;
            final int maxAttempts = 5;

            while (attempts < maxAttempts) {
                if (controllerServer.isRunning()) break;
                Thread.sleep(500 * attempts);
                attempts++;
            }

            assertTrue(
                    attempts < maxAttempts,
                    "Max attempts reached. Failed to initialise controller server"
            );

            // Create brokers
            ArrayList<RaftNode> nodes = new ArrayList<>();
            final int numBrokers = 3;

            for (int i = 0; i < numBrokers; i++) {
                nodes.add(new RaftNode(i, new InetSocketAddress("localhost", 9000 + i), null, NodeState.BROKER));
            }

            nodes.add(new RaftNode(
                    numBrokers,
                    new InetSocketAddress(controllerServer.getHost(), controllerServer.getPort()),
                    null,
                    NodeState.CONTROLLER
            ));

            // Creating broker managers
            for (int i = 0; i < numBrokers; i++) {
                // Configuring store and logs
                Path logPath = logsFolderPath.resolve(String.format("integration-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);
                logHandler.setTerm(1L);

                Path dir = snapshotFolderPath.resolve(String.format("%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);

                KVStore store = createStore(logHandler, snapshotter);

                // Ensuring first node will be victorious in the election
                if (i == 0) {
                    logHandler.setTerm(1L);
                    store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
                    store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
                    store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
                } else {
                    logHandler.setTerm(1L);
                    store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
                }

                RaftManager manager = new RaftManager(i, nodes, store);
                managers.add(manager);
                manager.start();
            }

            // Waiting for all brokers to startup
            Thread.sleep(500);

            attempts = 0;
            while (attempts < maxAttempts) {
                boolean shouldBreak = true;

                for (RaftManager manager : managers) {
                    shouldBreak = shouldBreak && manager.isRunning();
                }

                if (shouldBreak) {
                    break;
                }

                Thread.sleep(500 * attempts);
                attempts++;
            }

            assertTrue(attempts < maxAttempts, "Max attempts reached");

            // Initiating election with the first manager
            RaftManager manager0 = managers.get(0);
            manager0.initiateElectionAsControllerClient();

            // Waiting for election to play out
            Thread.sleep(5000);

            // Assertions
            assertEquals(NodeState.CONTROLLER, manager0.getState(), "Manager 0 should now be CONTROLLER");
            assertEquals(NodeState.BROKER, managers.get(1).getState(), "Manager 1 should be FOLLOWER");
            assertEquals(NodeState.BROKER, managers.get(2).getState(), "Manager 2 should be FOLLOWER");
            assertNull(manager0.getBrokerServerHandler(), "Broker server must be null");
            assertNotNull(manager0.getControllerServerHandler(), "Controller server must be running");
            assertEquals(
                    managers.get(0).getConfig().nodeAddress(),
                    managers.get(2).getControllerClient().getControllerAddress(),
                    "Manager 2 should have a controller client connection to Manager 0 as it's controller"
            );
            assertEquals(
                    managers.get(0).getConfig().nodeAddress(),
                    managers.get(1).getControllerClient().getControllerAddress(),
                    "Manager 1 should have a controller client connection to Manager 0 as it's controller"
            );
        } finally {
            for (RaftManager manager : managers) {
                manager.stop();
            }
            controllerServer.stop();
            executor.shutdownNow();
            boolean success = executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            if (!success) {
                System.err.println(debugPrefix + "Executor termination failed");
            }
        }
    }

    /**
     * Tests a more complex election scenario with 3 nodes:
     * 1. Election initiated with multiple potential voters
     * 2. First vote denied (simulating broker already voted for another candidate)
     * 3. Additional votes come in until majority is reached
     * 4. Election eventually succeeds despite initial rejection
     */
    @Test
    @DisplayName("Election succeeds despite initial vote denial")
    void electionSucceeds_despiteInitialVoteDenial() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[electionSucceeds_despiteInitialVoteDenial] ";
        ExecutorService executor = Executors.newFixedThreadPool(4);
        ArrayList<RaftManager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8998);

        try {
            // Starting controller server so brokers have something to connect to.
            executor.execute(() -> Utils.runnableWrapper(controllerServer::start));

            final int maxAttempts = 5;
            int attempts = 0;

            while (attempts < maxAttempts) {
                if (controllerServer.isRunning()) break;
                Thread.sleep(500 * attempts);
                attempts++;
            }

            assertTrue(
                    attempts < maxAttempts,
                    "Max attempts reached. Failed to initialise controller server"
            );

            ArrayList<RaftNode> nodes = new ArrayList<>();
            final int numBrokers = 3;
            for (int i = 0; i < numBrokers; i++) {
                nodes.add(new RaftNode(
                        i,
                        new InetSocketAddress("localhost", 9000 + i),
                        null,
                        NodeState.BROKER
                ));
            }

            nodes.add(new RaftNode(
                    numBrokers,
                    new InetSocketAddress(controllerServer.getHost(), controllerServer.getPort()),
                    null,
                    NodeState.CONTROLLER
            ));

            // Creating broker managers
            for (int i = 0; i < numBrokers; i++) {
                // Configuring store and logs
                Path logPath = logsFolderPath.resolve(String.format("integration-2-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path dir = snapshotFolderPath.resolve(String.format("integration-2-%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
                logHandler.setTerm(3L);
                logHandler.setLogId(10L);
                KVStore store = createStore(logHandler, snapshotter);

                // Creating manager
                RaftManager manager = new RaftManager(i, nodes, store);
                managers.add(manager);
                executor.execute(() -> Utils.runnableWrapper(manager::start));
            }

            // Waiting for brokers to spin up
            Thread.sleep(500);

            attempts = 0;
            while (attempts < maxAttempts) {
                boolean shouldBreak = true;

                for (RaftManager manager : managers) {
                    shouldBreak = shouldBreak && manager.isRunning();
                }

                if (shouldBreak) {
                    break;
                }

                Thread.sleep(500 * attempts);
                attempts++;
            }

            assertTrue(attempts < maxAttempts, "Max attempts reached waiting for managers to start");

            // Initiating election with the first manager
            RaftManager manager0 = managers.get(0);
            manager0.initiateElectionAsControllerClient();

            // Verify initial election state with 2 running broker clients, majority should be 2
            RaftManager.ElectionMeta meta = manager0.getElectionMeta();
            assertNotNull(meta, "Election metadata should exist after initiation");
            assertEquals(4L, meta.getTerm(), "Term should be 3 + 1 = 4");
            assertEquals(1, meta.getVoteCount(), "Should start with self-vote");
            assertEquals(2, meta.getMajority(), "Majority should be 2 (2 running brokers / 2 + 1)");

            // Simulate denied vote (broker already voted for someone else)
            RequestVoteResponse deniedVote = new RequestVoteResponse(false, 4L);
            manager0.handleVoteResponse(deniedVote);

            // Vote count unchanged, election still in progress
            assertEquals(1, manager0.getElectionMeta().getVoteCount(),
                    "Denied vote should not change vote count");
            assertEquals(NodeState.CANDIDATE, manager0.getState(),
                    "Should remain CANDIDATE after denied vote");

            // This should reach majority (2 votes out of 2 needed)
            RequestVoteResponse grantedVote = new RequestVoteResponse(true, 4L);
            manager0.handleVoteResponse(grantedVote);

            assertEquals(
                    NodeState.CONTROLLER,
                    manager0.getState(),
                    "Manager 0 should now be elected leader with 2 out of 2 needed votes to satisfy the majority."
            );

        } finally {
            for (RaftManager manager : managers) {
                manager.stop();
            }
            controllerServer.stop();
            executor.shutdown();
            boolean success = executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            if (!success) {
                System.err.println(debugPrefix + "Executor termination failed");
            }
        }
    }


    /**
     * Multiple elections with increasing terms
     * <p>
     * Tests that multiple election cycles work correctly with a 3-node cluster:
     * - Each election increments the term
     * - Vote granting respects the highest seen term
     * - A second election can be initiated after the first completes
     */
    @Test
    @DisplayName("Multiple election cycles with increasing terms")
    void multipleElections_withIncreasingTerms() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[multipleElections_withIncreasingTerms] ";

        ExecutorService executor = Executors.newFixedThreadPool(3);
        ArrayList<RaftManager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8996);

        final int maxAttempts = 5;

        try {
            executor.execute(() -> Utils.runnableWrapper(controllerServer::start));

            int attempts = 0;
            while (attempts < maxAttempts) {
                if (controllerServer.isRunning()) break;
                Thread.sleep(500 * attempts);
                attempts++;
            }

            assertTrue(
                    attempts < maxAttempts,
                    "Max attempts reached. Failed to initialise controller server"
            );

            ArrayList<RaftNode> nodes = new ArrayList<>();
            final int numBrokers = 3;

            for (int i = 0; i < numBrokers; i++) {
                nodes.add(new RaftNode(
                        i,
                        new InetSocketAddress("localhost", 9000 + i),
                        null,
                        NodeState.BROKER
                ));
            }

            nodes.add(new RaftNode(
                    numBrokers,
                    new InetSocketAddress(controllerServer.getHost(), controllerServer.getPort()),
                    null,
                    NodeState.CONTROLLER
            ));

            // Creating broker managers
            for (int i = 0; i < numBrokers; i++) {
                // Configuring store and logs
                Path logPath = logsFolderPath.resolve(String.format("integration-4-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path dir = snapshotFolderPath.resolve(String.format("integration-4-%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
                logHandler.setTerm(1L);
                KVStore store = createStore(logHandler, snapshotter);

                // Creating manager
                RaftManager manager = new RaftManager(i, nodes, store);
                managers.add(manager);
                executor.execute(() -> Utils.runnableWrapper(manager::start));
            }

            Thread.sleep(500);

            attempts = 0;
            while (attempts < maxAttempts) {
                boolean shouldBreak = true;

                for (RaftManager manager : managers) {
                    shouldBreak = shouldBreak && manager.isRunning();
                }

                if (shouldBreak) {
                    break;
                }

                Thread.sleep(500 * attempts);
                attempts++;
            }

            assertTrue(attempts < maxAttempts, "Max attempts reached waiting for managers to start");

            // First election cycle
            RaftManager manager0 = managers.get(0);
            manager0.initiateElectionAsControllerClient();

            // Verify first election state
            RaftManager.ElectionMeta meta = manager0.getElectionMeta();
            assertNotNull(meta, "Election metadata should exist for first election");
            assertEquals(2L, meta.getTerm(), "First election should use term 2 (1 + 1)");
            assertEquals(2, meta.getMajority(), "Majority should be 2 with 2 running broker clients");

            // Complete first election with a granted vote
            RequestVoteResponse firstVote = new RequestVoteResponse(true, 2L);
            manager0.handleVoteResponse(firstVote);

            Thread.sleep(1000);

            // Verify first election completed
            assertEquals(NodeState.CONTROLLER, manager0.getState(),
                    "Manager 0 should be CONTROLLER after first election");
            assertEquals(NodeState.BROKER, managers.get(1).getState(),
                    "Manager 1 should be follower after first election");
            // If this passes, we can safely assume that the second node will also have applied
            // the change. Of course in a remote distributed environment this can't be guaranteed
            // as nodes may be slow.
            assertEquals(2, ((RaftLogHandler) managers.get(1).getKVStore().getLogHandler()).getTerm(),
                    "Manager 1 should have updated their log handler's term to 2");
            assertEquals(NodeState.BROKER, managers.get(2).getState(),
                    "Manager 2 should be follower after first election");

            // Second election cycle
            // For a second election, we need to simulate a scenario where a new election is needed.
            // We'll use manager1 to initiate a new election with an updated term.
            RaftManager manager1 = managers.get(1);

            // Update the term in manager 1's log handler to simulate term progression
            RaftLogHandler logHandler1 = (RaftLogHandler) manager1.getControllerClient().getLogHandler();
            logHandler1.setTerm(2L);

            // Initiate second election from manager1
            manager1.initiateElectionAsControllerClient();

            // Verify second election state
            RaftManager.ElectionMeta meta2 = manager1.getElectionMeta();
            assertNotNull(meta2, "Election metadata should exist for second election");
            assertEquals(3L, meta2.getTerm(), "Second election should use term 3 (2 + 1)");
            assertEquals(2, meta2.getMajority(), "Majority should still be 2 with 2 running broker clients");

            // Complete second election with granted votes
            RequestVoteResponse secondVote = new RequestVoteResponse(true, 3L);
            manager1.handleVoteResponse(secondVote);

            // Verify second election completed
            assertEquals(NodeState.CONTROLLER, manager1.getState(),
                    "Manager 1 should be CONTROLLER after second election");

        } finally {
            for (RaftManager manager : managers) {
                manager.stop();
            }

            controllerServer.stop();
            executor.shutdown();
            boolean success = executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            if (!success) {
                System.err.println(debugPrefix + "Executor termination failed");
            }
        }
    }


    /**
     * Heartbeat server as a replacement for a full RaftControllerServer
     */
    private class TestControllerServer {
        private final String host = "localhost";
        private final int port;

        private volatile boolean running = false;
        private Selector selector;
        private ServerSocketChannel serverSocketChannel;
        private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();

        private final String DBG_PREFIX = "[TestControllerServer]";

        public TestControllerServer(int port) {
            this.port = port;
        }

        /**
         * Starts the server in its own thread.
         * Safe to call multiple times.
         */
        public synchronized void start() throws IOException {
            final String debugPrefix = DBG_PREFIX + "[start] ";

            System.out.println(debugPrefix + "starting test controller");
            if (running) {
                return;
            }

            running = true;
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(host, port));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (running) {
                int readyCount = selector.select(100);
                if (readyCount == 0) continue;

                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (!key.isValid()) continue;

                    try {
                        if (key.isAcceptable()) {
                            handleAccept(key);
                        } else if (key.isReadable()) {
                            handleRead(key);
                        } else if (key.isWritable()) {
                            handleWrite(key);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        cleanup(key);
                    }
                }
            }

            System.out.println(debugPrefix + "stopping");
        }

        /**
         * Stops the server whether running in a thread or not.
         */
        public void stop() throws IOException {
            final String debugPrefix = DBG_PREFIX + "[stop]";

            if (!running) {
                return;
            }

            running = false;

            for (SelectionKey key : selector.keys()) {
                try {
                    cleanup(key);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


            try {
                selector.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }

            try {
                serverSocketChannel.close();
            } catch (Exception e) {
                System.out.println(debugPrefix + "Exception during serverSocketChannel.close()");
                e.printStackTrace();
                throw e;
            }
        }

        private void handleAccept(SelectionKey key) throws IOException {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel client = server.accept();
            if (client == null) {
                return;
            }

            client.configureBlocking(false);
            client.socket().setTcpNoDelay(true);
            client.socket().setKeepAlive(true);

            SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
            clientKey.attach(new ClientSession(client));
        }

        private void handleRead(SelectionKey key) throws IOException {
            SocketChannel channel = (SocketChannel) key.channel();
            ClientSession session = (ClientSession) key.attachment();
            ByteBuffer readBuffer = session.getReadBuffer();


            int readCount = channel.read(readBuffer);

            if (readCount == -1) {
                cleanup(key);
                return;
            }

            if (readCount == 0) {
                return;
            }

            int position = readBuffer.position();
            readBuffer.flip();
            boolean processed = processData(session, readBuffer);
            readBuffer.flip();

            if (processed) {
                readBuffer.compact();
            } else {
                readBuffer.position(position);
            }
        }

        private boolean processData(ClientSession session, ByteBuffer buffer) {
            final String debugPrefix = DBG_PREFIX + "[processData] ";


            BaseMessage message;
            try {
                message = BaseMessage.deserialize(buffer);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                System.out.println(debugPrefix + " Exit method returning true (exception path)");
                return true;
            }

            if (message == null) {
                return false;
            }

            MessageType messageType = message.type();

            ByteBuffer responseBuffer = null;

            if (messageType.equals(MessageType.HEARTBEAT_REQUEST)) {
                responseBuffer = ByteBuffer.wrap(new HeartbeatResponse().serialize());
            }

            if (responseBuffer != null) {
                queueWrite(session.getChannel(), responseBuffer);
            }

            return true;
        }

        private void queueWrite(SocketChannel client, ByteBuffer data) {
            Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(client, k -> new LinkedList<>());
            queue.offer(data.duplicate());

            SelectionKey key = client.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        }

        private void handleWrite(SelectionKey key) throws IOException {
            SocketChannel client = (SocketChannel) key.channel();
            Queue<ByteBuffer> queue = pendingWrites.get(client);

            if (queue == null || queue.isEmpty()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                return;
            }

            ByteBuffer buffer = queue.peek();
            while (buffer != null) {
                int bytesWritten = client.write(buffer);

                if (bytesWritten == 0) {
                    break;  // Send buffer full
                }

                if (!buffer.hasRemaining()) {
                    queue.poll();
                    buffer = queue.peek();
                }
            }

            if (queue.isEmpty()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        }

        private void cleanup(SelectionKey key) {
            if (key == null || !key.isValid()) return;

            try {

                SelectableChannel client = key.channel();
                pendingWrites.remove(client);

                key.cancel();
                try {
                    client.close();
                } catch (IOException e) {
                    // Ignore
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public boolean isRunning() {
            return serverSocketChannel != null && serverSocketChannel.isOpen();
        }
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }
}