package com.zenz.kvstore.server;

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
            manager0.initiateElection();

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
            manager0.initiateElection();

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
     * Tests edge case where a node receives a vote request with greater term but lower log id.
     * <p>
     * Scenario:
     * - 6 brokers total, majority = 4
     * - Broker 5 has a higher log id than others
     * - Broker 5 receives a vote request with greater term but lower log id
     * - Broker 5 should become CANDIDATE and send out vote requests
     * - Controller client should lose connection
     * - Since already in election, should NOT see another vote request triggered
     */
    @Test
    @DisplayName("Node receiving vote request with greater term but lower log id initiates election without duplicate")
    void nodeReceivingVoteRequest_withGreaterTermLowerLogId_initiatesElectionWithoutDuplicate() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[nodeReceivingVoteRequest_withGreaterTermLowerLogId_initiatesElectionWithoutDuplicate] ";

        ExecutorService executor = Executors.newFixedThreadPool(8);
        ArrayList<RaftManager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8994);
        TestControllerServer controllerServer1 = new TestControllerServer(8995);

        final int maxAttempts = 5;

        try {
            executor.execute(() -> Utils.runnableWrapper(controllerServer::start));
            executor.execute(() -> Utils.runnableWrapper(controllerServer1::start));

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

            // Create 6 brokers
            ArrayList<RaftNode> nodes = new ArrayList<>();
            final int numBrokers = 6;

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
                Path logPath = logsFolderPath.resolve(String.format("integration-6-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path dir = snapshotFolderPath.resolve(String.format("integration-6-%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
                KVStore store = createStore(logHandler, snapshotter);

                // Broker 5 has a higher log id (more entries) but same term
                RaftManager manager;

                if (i == 5) {
                    logHandler.setTerm(1L);
                    logHandler.setLogId(10L);
                    RaftNode node = nodes.removeLast();
                    nodes.add(new RaftNode(
                            node.id(),
                            new InetSocketAddress(controllerServer1.getHost(), controllerServer1.getPort()),
                            null,
                            NodeState.CONTROLLER
                    ));
                    ArrayList<RaftNode> list = new ArrayList<>(nodes.subList(0, 6));
                    list.add(new RaftNode(
                            node.id(),
                            new InetSocketAddress(controllerServer1.getHost(), controllerServer1.getPort()),
                            null,
                            NodeState.CONTROLLER
                    ));

                    manager = new RaftManager(i, list, store);
                } else {
                    logHandler.setTerm(1L);
                    logHandler.setLogId(3L);
                    manager = new RaftManager(i, nodes, store);
                }

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

            // Verify initial state
            RaftManager manager5 = managers.get(5);
            RaftLogHandler logHandler5 = (RaftLogHandler) manager5.getKVStore().getLogHandler();
            assertEquals(1L, logHandler5.getTerm(), "Broker 5 should have term 1");
            assertEquals(10L, logHandler5.getLogId(), "Broker 5 should have logId 10");

            // Verify controller client is running
            assertNotNull(manager5.getControllerClient(), "Broker 5 should have a controller client");
            assertTrue(manager5.getControllerClient().isRunning(), "Controller client should be running");

            // Simulate receiving a vote request with greater term but lower log id
            // Term = 5 (greater than broker 5's term of 1)
            // prevLogId = 3 (lower than broker 5's logId of 10)
            RequestVote voteRequest = new RequestVote(
                    0L,  // candidateId
                    5L,  // term (greater than broker 5's term)
                    3L,  // prevLogId (lower than broker 5's logId of 10)
                    1L   // prevTerm
            );

            // Process the vote request through the broker server handler
            // This should trigger initiateElection() because prevLogId < currentLogId
            manager5.setLastTerm(voteRequest.term());

            // Verify initial state before election
            assertNull(manager5.getElectionMeta(), "Broker 5 should not have election metadata before vote request");

            // Initiate election (simulating what RaftBrokerServerHandler would do)
            manager5.initiateElection();

            // Verify broker 5 is now CANDIDATE
            assertEquals(NodeState.CANDIDATE, manager5.getState(),
                    "Broker 5 should be CANDIDATE after receiving vote request with lower log id");

            // Verify election metadata
            RaftManager.ElectionMeta meta5 = manager5.getElectionMeta();
            assertNotNull(meta5, "Broker 5 should have election metadata after initiating election");
            assertEquals(6L, meta5.getTerm(), "Broker 5's election should use term 6 (lastTerm 5 + 1)");
            assertEquals(4, meta5.getMajority(), "Majority should be 4 with 6 brokers");
            assertEquals(1, meta5.getVoteCount(), "Should start with self-vote");

            // Store the original election term for comparison
            long originalElectionTerm = meta5.getTerm();

            // Now simulate the controller client detecting disconnection and calling initiateElection again
            // This should NOT create a new election since we're already in one
            controllerServer1.stop();

            // Verify election metadata hasn't changed (no new election started)
            RaftManager.ElectionMeta metaAfter = manager5.getElectionMeta();
            assertNotNull(metaAfter, "Election metadata should still exist");
            assertEquals(originalElectionTerm, metaAfter.getTerm(),
                    "Election term should not change when initiateElection called during ongoing election");
            assertEquals(1, metaAfter.getVoteCount(),
                    "Vote count should still be 1 (self-vote only, no reset)");

            // Verify broker clients are ready to send vote requests
            ArrayList<RaftBrokerClient> brokerClients = manager5.getBrokerClients();
            assertTrue(brokerClients.size() > 0, "Broker 5 should have broker clients");

            // Simulate votes coming in to complete the election
            RequestVoteResponse vote1 = new RequestVoteResponse(true, 6L);
            manager5.handleVoteResponse(vote1);

            RequestVoteResponse vote2 = new RequestVoteResponse(true, 6L);
            manager5.handleVoteResponse(vote2);

            RequestVoteResponse vote3 = new RequestVoteResponse(true, 6L);
            manager5.handleVoteResponse(vote3);

            // Now broker 5 should have 4 votes (1 self + 3 granted) = majority
            assertEquals(NodeState.CONTROLLER, manager5.getState(),
                    "Broker 5 should be CONTROLLER after reaching majority of 4 votes");

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
     * Tests election with 6 brokers where one broker has a higher log term.
     * <p>
     * Scenario:
     * - 6 brokers total, majority = 4
     * - Broker 5 has a higher log term than others
     * - When broker 5 receives a vote request with a smaller prevLogId,
     * it should reject the vote and initiate its own election with a larger term
     */
    @Test
    @DisplayName("Broker with higher log term initiates own election on receiving stale vote request")
    void brokerWithHigherLogTerm_initiatesOwnElection_onStaleVoteRequest() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[brokerWithHigherLogTerm_initiatesOwnElection_onStaleVoteRequest] ";

        ExecutorService executor = Executors.newFixedThreadPool(8);
        ArrayList<RaftManager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8995);

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

            // Create 6 brokers
            ArrayList<RaftNode> nodes = new ArrayList<>();
            final int numBrokers = 6;

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
                Path logPath = logsFolderPath.resolve(String.format("integration-5-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path dir = snapshotFolderPath.resolve(String.format("integration-5-%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);

                // Set base term for all brokers
                logHandler.setTerm(1L);

                // Broker 5 has a higher log term and more entries
                if (i == 5) {
                    logHandler.setTerm(3L);
                    logHandler.setLogId(10L);
                } else {
                    logHandler.setTerm(1L);
                    logHandler.setLogId(3L);
                }

                KVStore store = createStore(logHandler, snapshotter);

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

            // Verify initial state
            RaftManager manager5 = managers.get(5);
            RaftLogHandler logHandler5 = (RaftLogHandler) manager5.getKVStore().getLogHandler();
            assertEquals(3L, logHandler5.getTerm(), "Broker 5 should have term 3");
            assertEquals(10L, logHandler5.getLogId(), "Broker 5 should have logId 10");

            // Initiate election from broker 0 (which has lower log term)
            RaftManager manager0 = managers.get(0);
            manager0.initiateElection();

            // Wait for election to propagate
            Thread.sleep(2000);

            // Verify that broker 5 has seen the vote request and updated its lastTerm
            // The vote request from broker 0 should have term 2 (1 + 1)
            // Broker 5 should reject this because its logId (10) > prevLogId in the request (3)
            assertEquals(NodeState.BROKER, manager5.getState(),
                    "Broker 5 should remain BROKER after receiving stale vote request");

            // Broker 5 should have updated its lastTerm to the term from the vote request
            assertTrue(manager5.getLastTerm() >= 2L,
                    "Broker 5 should have updated its lastTerm");

            // Now simulate broker 5 initiating its own election
            // This would happen because it rejected the vote due to having a higher logId
            manager5.initiateElection();

            // Verify election metadata for broker 5
            RaftManager.ElectionMeta meta5 = manager5.getElectionMeta();
            assertNotNull(meta5, "Broker 5 should have election metadata after initiating election");
            // Term should be 4 (lastTerm 3 + 1)
            assertEquals(4L, meta5.getTerm(), "Broker 5's election should use term 4");
            assertEquals(4, meta5.getMajority(), "Majority should be 4 with 6 brokers");

            // Simulate votes coming in for broker 5's election
            // Need 4 votes total (including self-vote) to reach majority
            // After self-vote, need 3 more
            RequestVoteResponse vote1 = new RequestVoteResponse(true, 4L);
            manager5.handleVoteResponse(vote1);

            RequestVoteResponse vote2 = new RequestVoteResponse(true, 4L);
            manager5.handleVoteResponse(vote2);

            RequestVoteResponse vote3 = new RequestVoteResponse(true, 4L);
            manager5.handleVoteResponse(vote3);

            // Now broker 5 should have 4 votes (1 self + 3 granted) = majority
            assertEquals(NodeState.CONTROLLER, manager5.getState(),
                    "Broker 5 should be CONTROLLER after reaching majority of 4 votes");

            // Verify other brokers are still BROKERs
            for (int i = 1; i < 4; i++) {
                assertEquals(NodeState.BROKER, managers.get(i).getState(),
                        "Broker " + i + " should remain BROKER");
            }

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
            manager0.initiateElection();

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
            manager1.initiateElection();

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