package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.CheckedRunnable;
import com.zenz.kvstore.common.utils.Utils;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.RequestVoteResponse;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class LeaderElectionTest {

    private final String DEBUG_PREFIX = "[LeaderElectionTest]";
    private Path tempDir;
    private Path logsFolderPath;
    private Path snapshotFolderPath;

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

    private int getBasePort() {
        final Random random = new Random();
        return random.nextInt(1000, 9994);
    }

    /**
     * Creates a NodeConfig from a RaftNodeConfig's id and serverAddress.
     */
    private NodeConfig toNodeConfig(RaftNodeConfig node) {
        return new NodeConfig(String.valueOf(node.id()), node.nodeAddress(), null);
    }

    @Test
    @DisplayName("Tests that bootstrapping concludes with fresh nodes")
    void testBootstrapConcludes_withFreshNodes() throws IOException, InterruptedException {
        final int numNodes = 3;
        final ExecutorService executor = Executors.newFixedThreadPool(numNodes);
        final List<Manager> managers = new ArrayList<>();
        final List<NodeConfig> nodeConfigs = new ArrayList<>();

        final int basePort = getBasePort();
        for (int i = 0; i < numNodes; i++) {
            nodeConfigs.add(new NodeConfig("Node-" + i, new InetSocketAddress("localhost", basePort + i), null));
        }

        try {
            for (int i = 0; i < numNodes; i++) {
                Path logPath = this.logsFolderPath.resolve(String.format("integration-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path snapshotDir = this.snapshotFolderPath.resolve(String.format("snapshots-%s", i));
                Files.createDirectories(snapshotDir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(snapshotDir);

                final KVStore kvStore = createStore(logHandler, snapshotter);
                final NodeConfig nodeConfig = nodeConfigs.get(i);
                final List<NodeConfig> peerConfigs = nodeConfigs.stream().filter((config) -> config != nodeConfig).toList();
                final Manager manager = new Manager(kvStore, nodeConfig, peerConfigs);
                managers.add(manager);
                manager.setLoggingEnabled(false);
                executor.submit(() -> {
                    try {
                        manager.start();
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            waitForManagersNew(managers);
            final int controllerCount = (int) managers
                    .stream()
                    .filter((manager) -> manager.getRole() == NodeRole.CONTROLLER)
                    .count();

            Assertions.assertEquals(1, controllerCount, "A controller should've been established");

            final int brokerCount = (int) managers
                    .stream()
                    .filter((manager) -> manager.getRole() == NodeRole.BROKER)
                    .count();

            Assertions.assertEquals(2, brokerCount, "The rest of the cluster should all be brokers");

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            for (Manager manager : managers) {
                manager.stop();
            }

            executor.shutdownNow();
            boolean success = executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            if (!success) {
                System.err.println("Executor termination failed");
            }
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
        ArrayList<Manager> managers = new ArrayList<>();

        TestControllerServer controllerServer = new TestControllerServer(8999);

        try {
            // Starting controller server so brokers have something to connect to
            executor.execute(() -> Utils.checkedRunnableWrapper(controllerServer::start));

            assertTrue(
                    waitForController(controllerServer),
                    "Max attempts reached. Failed to initialise controller server"
            );

            // Create brokers
            final int numBrokers = 3;
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < numBrokers; i++) {
                nodeConfigs.add(
                        new NodeConfig("" + i, new InetSocketAddress("localhost", basePort + i), null)
                );
            }

            // Creating broker managers
            for (int i = 0; i < numBrokers; i++) {
                // Configuring store and logs
                Path logPath = logsFolderPath.resolve(String.format("integration-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path snapshotDir = snapshotFolderPath.resolve(String.format("snapshots-%s", i));
                Files.createDirectories(snapshotDir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(snapshotDir);

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

                NodeConfig nodeConfig = nodeConfigs.get(i);
                Manager manager = new Manager(
                        store,
                        nodeConfig,
                        nodeConfigs.stream().filter(config -> config != nodeConfig).toList()
                );
                manager.setLoggingEnabled(false);
                managers.add(manager);
                executor.submit(() -> {
                    try {
                        manager.start();
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Waiting for all managers to startup
            assertTrue(waitForManagersNew(managers), "Max attempts reached. Managers failed to initialise");

            // Initiating election with the first manager
            Manager manager0 = managers.getFirst();
            manager0.startElection();

            // Waiting for election to play out
            Thread.sleep(3_000);

            // Assertions
            assertEquals(NodeRole.CONTROLLER, manager0.getRole(), "Manager 0 should now be CONTROLLER");
            assertEquals(NodeRole.BROKER, managers.get(1).getRole(), "Manager 1 should be FOLLOWER");
            assertEquals(NodeRole.BROKER, managers.get(2).getRole(), "Manager 2 should be FOLLOWER");
            assertTrue(manager0.getServer().isRunning(), "Server must be running");
            InetSocketAddress address0 = manager0.getNodeConfig().serverAddress();
            ControllerClient controllerClient2 = managers.get(2).getControllerClient();
            assertNotNull(controllerClient2, "Manager 2 controller should have a controller client connected to manager 0");
            assertEquals(
                    address0,
                    controllerClient2.getRemoteAddress(),
                    "Manager 2 should have a controller client connection to Manager 0 as it's controller"
            );
            ControllerClient controllerClient1 = managers.get(1).getControllerClient();
            assertNotNull(controllerClient1, "Manager 1 controller should have a controller client connected to manager 0");
            assertEquals(
                    address0,
                    controllerClient1.getRemoteAddress(),
                    "Manager 1 should have a controller client connection to Manager 0 as it's controller"
            );
        } finally {
            for (Manager manager : managers) {
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
        ArrayList<Manager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8998);

        try {
            // Starting controller server so brokers have something to connect to.
            executor.execute(() -> Utils.checkedRunnableWrapper(controllerServer::start));

            assertTrue(waitForController(controllerServer), "Max attempts reached. Failed to initialise controller server");

            final int numBrokers = 3;

            // Create node configs (brokers only - no controller in the list)
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < numBrokers; i++) {
                nodeConfigs.add(new NodeConfig("" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

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
                NodeConfig nodeConfig = nodeConfigs.get(i);
                ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
                nodeConfigsCopy.remove(nodeConfig);
                Manager manager = new Manager(store, nodeConfig, nodeConfigsCopy);
                manager.setLoggingEnabled(false);
                managers.add(manager);
                executor.submit(() -> {
                    try {
                        manager.start();
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Waiting for all managers to startup
            assertTrue(waitForManagersNew(managers), "Max attempts reached waiting for managers to start");

            // Initiating election with the first manager
            Manager manager0 = managers.get(0);
            manager0.startElection();

            // Verify initial election state with 2 running broker clients, majority should be 2
            Manager.Election meta = manager0.getElection();
            assertNotNull(meta, "Election metadata should exist after initiation");
//            assertEquals(1L, meta.getTerm(), "Term should be 1 (0 + 1)");
            assertEquals(1, meta.getVoteCount(), "Should start with self-vote");
            assertEquals(2, meta.getMajority(), "Majority should be 2 (2 running brokers / 2 + 1)");

            // Simulate denied vote (broker already voted for someone else)
            RequestVoteResponse deniedVote = new RequestVoteResponse(false, meta.getTerm());
            manager0.handleVoteResponse(deniedVote);

            // Vote count unchanged, election still in progress
            assertEquals(1, manager0.getElection().getVoteCount(),
                    "Denied vote should not change vote count");
            assertEquals(NodeRole.CANDIDATE, manager0.getRole(),
                    "Should remain CANDIDATE after denied vote");

            // This should reach majority (2 votes out of 2 needed)
            RequestVoteResponse grantedVote = new RequestVoteResponse(true, meta.getTerm());
            manager0.handleVoteResponse(grantedVote);

            assertEquals(
                    NodeRole.CONTROLLER,
                    manager0.getRole(),
                    "Manager 0 should now be elected leader with 2 out of 2 needed votes to satisfy the majority."
            );

        } finally {
            for (Manager manager : managers) {
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
    @DisplayName("Manager receiving vote request with greater term but lower log id initiates election without duplicate")
    void nodeReceivingVoteRequest_withGreaterTermLowerLogId_initiatesElectionWithoutDuplicate() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[nodeReceivingVoteRequest_withGreaterTermLowerLogId_initiatesElectionWithoutDuplicate] ";

        ExecutorService executor = Executors.newFixedThreadPool(8);
        ArrayList<Manager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8994);

        try {
            executor.execute(() -> Utils.checkedRunnableWrapper(controllerServer::start));
            assertTrue(waitForController(controllerServer), "Max attempts reached. Failed to initialise controller server");

            // Create 6 brokers
            final int numBrokers = 6;

            // Create node configs (brokers only)
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < numBrokers; i++) {
                nodeConfigs.add(new NodeConfig("" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

            // Creating broker managers
            for (int i = 0; i < numBrokers; i++) {
                Path logPath = logsFolderPath.resolve(String.format("node-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path dir = snapshotFolderPath.resolve(String.format("node-%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);

                // Broker 5 has a higher log id (more entries)
                if (i == 5) {
                    logHandler.setTerm(1L);
                    logHandler.setLogId(10L);
                } else {
                    logHandler.setTerm(1L);
                    logHandler.setLogId(3L);
                }

                KVStore store = createStore(logHandler, snapshotter);

                NodeConfig nodeConfig = nodeConfigs.get(i);
                ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
                nodeConfigsCopy.remove(nodeConfig);
                Manager manager = new Manager(store, nodeConfig, nodeConfigsCopy);
                manager.setLoggingEnabled(false);
                managers.add(manager);
                final List<NodeConfig> finalConfigs = nodeConfigsCopy;
                executor.submit(() -> {
                    try {
                        manager.start();
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Waiting for all managers to startup
            assertTrue(waitForManagersNew(managers), "Max attempts reached waiting for managers to start");

            // Verify initial state
            Manager manager5 = managers.get(5);
            RaftLogHandler logHandler5 = (RaftLogHandler) manager5.getKVStore().getLogHandler();
//            assertEquals(1L, logHandler5.getTerm(), "Broker 5 should have term 1");
            assertEquals(10L, logHandler5.getLogId(), "Broker 5 should have logId 10");

            // Simulate receiving a vote request with greater term but lower log id
            manager5.setCurrentTerm(manager5.getCurrentTerm() + 1);

            // Verify initial state before election
//            assertNull(manager5.getElection(), "Broker 5 should not have election metadata before vote request");

            // Initiate election (simulating what RaftBrokerServerHandler would do)
            final long prevTerm = manager5.getCurrentTerm();
            manager5.startElection();

            // Verify broker 5 is now CANDIDATE
            assertEquals(NodeRole.CANDIDATE, manager5.getRole(),
                    "Broker 5 should be CANDIDATE after receiving vote request with lower log id");

            // Verify election metadata
            Manager.Election meta5 = manager5.getElection();
            assertNotNull(meta5, "Broker 5 should have election metadata after initiating election");
            assertEquals(prevTerm + 1, meta5.getTerm(), "Broker 5's election should use term 6 (lastTerm 5 + 1)");
            assertEquals(4, meta5.getMajority(), "Majority should be 4 with 6 brokers");
            assertEquals(1, meta5.getVoteCount(), "Should start with self-vote");

            // Store the original election term for comparison
            final long originalElectionTerm = meta5.getTerm();

            // Verify election metadata hasn't changed (no new election started)
            Manager.Election metaAfter = manager5.getElection();
            assertNotNull(metaAfter, "Election metadata should still exist");
            assertEquals(originalElectionTerm, metaAfter.getTerm(),
                    "Election term should not change when initiateElection called during ongoing election");
            assertEquals(1, metaAfter.getVoteCount(),
                    "Vote count should still be 1 (self-vote only, no reset)");

            // Verify broker clients are ready to send vote requests
//            ArrayList<RaftBrokerClient> brokerClients = manager5.getBrokerClients();
//            assertTrue(brokerClients.size() > 0, "Broker 5 should have broker clients");

            // Simulate votes coming in to complete the election
            RequestVoteResponse vote1 = new RequestVoteResponse(true, metaAfter.getTerm());
            manager5.handleVoteResponse(vote1);

            RequestVoteResponse vote2 = new RequestVoteResponse(true, metaAfter.getTerm());
            manager5.handleVoteResponse(vote2);

            RequestVoteResponse vote3 = new RequestVoteResponse(true, metaAfter.getTerm());
            manager5.handleVoteResponse(vote3);

            // Now broker 5 should have 4 votes (1 self + 3 granted) = majority
            assertEquals(NodeRole.CONTROLLER, manager5.getRole(),
                    "Broker 5 should be CONTROLLER after reaching majority of 4 votes");

        } finally {
            for (Manager manager : managers) {
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

        ExecutorService executor = Executors.newFixedThreadPool(4);
        ArrayList<Manager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8996);

        try {
//            executor.execute(() -> Utils.runnableWrapper(controllerServer::start));
//            assertTrue(waitForController(controllerServer), "Max attempts reached. Failed to initialise controller server");

            final int numBrokers = 3;

            // Create node configs (brokers only)
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < numBrokers; i++) {
                nodeConfigs.add(new NodeConfig("" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

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

                // Add some initial data to help with initialization
                store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

                // Creating manager
                NodeConfig nodeConfig = nodeConfigs.get(i);
                ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
                nodeConfigsCopy.remove(nodeConfig);
                Manager manager = new Manager(store, nodeConfig, nodeConfigsCopy);
//                manager.setLoggingEnabled(false);
                managers.add(manager);
                final List<NodeConfig> finalConfigs = nodeConfigsCopy;
                executor.submit(() -> {
                    try {
                        manager.start();
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Waiting for all managers to startup
            assertTrue(waitForManagersNew(managers), "Max attempts reached waiting for managers to start");
            final long term = managers.getFirst().getCurrentTerm();
            long controllerCount = managers.stream().filter((m) -> m.getRole() == NodeRole.CONTROLLER).count();
            assertEquals(
                    1,
                    controllerCount,
                    "Only one node should be the controller. Found " + controllerCount + " controllers"
            );

            // First election cycle
            managers.get(1).setRole(NodeRole.BROKER);
            Manager manager0 = managers.get(0);
            manager0.startElection();

            // Verify first election state
            Manager.Election meta = manager0.getElection();
            assertNotNull(meta, "Election metadata should exist for first election");
            assertEquals(term + 1, meta.getTerm(), "First election should use term 2 as nodes had to agree on leader");
            assertEquals(2, meta.getMajority(), "Majority should be 2 with 2 running broker clients");

            // Complete first election with a granted vote
            RequestVoteResponse firstVote = new RequestVoteResponse(true, meta.getTerm());
            manager0.handleVoteResponse(firstVote);

            Thread.sleep(3000);

            // Verify first election completed
            assertEquals(NodeRole.CONTROLLER, manager0.getRole(),
                    "Manager 0 should be CONTROLLER after second election");
            assertEquals(NodeRole.BROKER, managers.get(1).getRole(),
                    "Manager 1 should be follower after second election");
            assertEquals(NodeRole.BROKER, managers.get(2).getRole(),
                    "Manager 2 should be follower after second election");

            // Second election cycle
            // For a second election, we need to simulate a scenario where a new election is needed.
            // We'll use manager1 to initiate a new election with an updated term.
            Manager manager1 = managers.get(1);

            // Update the term in manager 1's log handler to simulate term progression
            RaftLogHandler logHandler1 = (RaftLogHandler) manager1.getKVStore().getLogHandler();
            logHandler1.setTerm(2L);

            // Initiate second election from manager1
            manager1.startElection();

            // Verify second election state
            Manager.Election meta2 = manager1.getElection();
            assertNotNull(meta2, "Election metadata should exist for second election");
            assertEquals(meta.getTerm() + 1, meta2.getTerm(), "Second election should use term 3 (2 + 1)");
            assertEquals(2, meta2.getMajority(), "Majority should still be 2 with 2 running broker clients");

            // Complete second election with granted votes
            RequestVoteResponse secondVote = new RequestVoteResponse(true, meta2.getTerm());
            manager1.handleVoteResponse(secondVote);

            // Verify second election completed
            assertEquals(NodeRole.CONTROLLER, manager1.getRole(),
                    "Manager 1 should be CONTROLLER after second election");

        } finally {
            for (Manager manager : managers) {
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
     * Unit test for broker connection failure during election.
     * <p>
     * Tests that when a broker connection fails during an election,
     * the vote count is incremented as if the broker voted for us.
     * This effectively reduces the majority threshold, allowing the
     * election to complete with fewer actual votes.
     */
    @Test
    @DisplayName("Broker connection failure increments vote count during election")
    void brokerConnectionFailure_incrementsVoteCount_duringElection() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[brokerConnectionFailure_incrementsVoteCount_duringElection] ";

        ExecutorService executor = Executors.newFixedThreadPool(4);
        ArrayList<Manager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8997);

        try {
            executor.submit(() -> Utils.checkedRunnableWrapper(controllerServer::start));
            assertTrue(waitForController(controllerServer), "Max attempts reached. Failed to initialise controller server");

            // Create 3 brokers
            final int numBrokers = 3;

            // Create node configs (brokers only)
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < numBrokers; i++) {
                nodeConfigs.add(new NodeConfig("" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

            // Creating broker managers
            for (int i = 0; i < numBrokers; i++) {
                Path logPath = logsFolderPath.resolve(String.format("broker-failure-unit-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path dir = snapshotFolderPath.resolve(String.format("broker-failure-unit-%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
                logHandler.setTerm(1L);
                KVStore store = createStore(logHandler, snapshotter);

                final NodeConfig nodeConfig = nodeConfigs.get(i);
//                ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
//                nodeConfigsCopy.remove(nodeConfig);

                Manager manager = new Manager(
                        store,
                        nodeConfig,
                        nodeConfigs.stream().filter(config -> config != nodeConfig).toList()
                );
                manager.setLoggingEnabled(false);
                managers.add(manager);

                executor.submit(() -> {
                    try {
                        manager.start();
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Waiting for all managers to startup
            assertTrue(waitForManagersNew(managers), "Max attempts reached waiting for managers to start");
            final long term = managers.getFirst().getCurrentTerm();

            // Initiate election from manager 0
            Manager manager0 = managers.get(0);
            manager0.startElection();

            // Verify initial election state
            Manager.Election meta = manager0.getElection();
            assertNotNull(meta, "Election metadata should exist after initiation");
            assertEquals(term + 1, meta.getTerm(), "Term should be 1 (0 + 1)");
            assertEquals(1, meta.getVoteCount(), "Should start with self-vote");
            assertEquals(2, meta.getMajority(), "Majority should be 2 with 2 running broker clients");

            // Simulate broker connection failure by directly calling handleVoteResponse
            // This simulates what happens in RaftBrokerClient.handleRead() when bytesRead == -1
            RequestVoteResponse simulatedFailureVote = new RequestVoteResponse(true, meta.getTerm());
            manager0.handleVoteResponse(simulatedFailureVote);

            // Vote count should now be 2 (self-vote + failed broker treated as granted)
            assertEquals(2, manager0.getElection().getVoteCount(),
                    "Vote count should increment when broker connection fails");

            // Election should complete since we reached majority
            assertEquals(NodeRole.CONTROLLER, manager0.getRole(),
                    "Manager 0 should be CONTROLLER after reaching majority via failed broker connection");

        } finally {
            for (Manager manager : managers) {
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
     * Integration test for election completion with multiple broker connection failures.
     * <p>
     * Tests that an election can complete successfully when multiple broker
     * connections fail during the election process. The failed brokers are
     * treated as having voted for the candidate, effectively reducing the
     * majority threshold.
     * <p>
     * Scenario:
     * - 5 brokers total, initial majority = 3
     * - 2 broker connections fail during election
     * - With 2 failed brokers, effective majority becomes 2
     * - Candidate wins with self-vote + 1 actual vote + 2 failed broker "votes"
     */
    @Test
    @DisplayName("Election completes with multiple broker connection failures")
    void electionCompletes_withMultipleBrokerConnectionFailures() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[electionCompletes_withMultipleBrokerConnectionFailures] ";

        ExecutorService executor = Executors.newFixedThreadPool(8);
        ArrayList<Manager> managers = new ArrayList<>();
        TestControllerServer controllerServer = new TestControllerServer(8991);

        try {
            executor.execute(() -> Utils.checkedRunnableWrapper(controllerServer::start));
            assertTrue(waitForController(controllerServer), "Max attempts reached. Failed to initialise controller server");

            // Create 5 brokers
            final int numBrokers = 5;

            // Create node configs (brokers only)
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < numBrokers; i++) {
                nodeConfigs.add(new NodeConfig("" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

            // Creating broker managers
            for (int i = 0; i < numBrokers; i++) {
                Path logPath = logsFolderPath.resolve(String.format("multi-failure-%s.log", i));
                WALogger logger = new WALogger(logPath);
                RaftLogHandler logHandler = new RaftLogHandler(logger);

                Path dir = snapshotFolderPath.resolve(String.format("multi-failure-%s-snapshots", i));
                Files.createDirectories(dir);
                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
                logHandler.setTerm(1L);
                KVStore store = createStore(logHandler, snapshotter);

                NodeConfig nodeConfig = nodeConfigs.get(i);
                ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
                nodeConfigsCopy.remove(nodeConfig);
                Manager manager = new Manager(store, nodeConfig, nodeConfigsCopy);
                manager.setLoggingEnabled(false);
                managers.add(manager);
                final List<NodeConfig> finalConfigs = nodeConfigsCopy;
                executor.submit(() -> {
                    try {
                        manager.start();
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Waiting for all managers to startup
            assertTrue(waitForManagersNew(managers), "Max attempts reached waiting for managers to start");
            final long startTerm = managers.getFirst().getCurrentTerm();

            // Initiate election from manager 0
            Manager manager0 = managers.get(0);
            manager0.startElection();

            // Verify initial election state
            // With 5 brokers, we have 4 broker clients + self = 5 total
            // Majority = 5 / 2 + 1 = 3
            Manager.Election meta = manager0.getElection();
            assertNotNull(meta, "Election metadata should exist after initiation");
            assertEquals(startTerm + 1, meta.getTerm(), "Term should be 1 (0 + 1)");
            assertEquals(1, meta.getVoteCount(), "Should start with self-vote");
            assertEquals(3, meta.getMajority(), "Majority should be 3 with 4 running broker clients + self");

            // Simulate first broker connection failure
            RequestVoteResponse failureVote1 = new RequestVoteResponse(true, meta.getTerm());
            manager0.handleVoteResponse(failureVote1);

            assertEquals(2, manager0.getElection().getVoteCount(),
                    "Vote count should be 2 after first failure");

            // Simulate second broker connection failure
            RequestVoteResponse failureVote2 = new RequestVoteResponse(true, meta.getTerm());
            manager0.handleVoteResponse(failureVote2);

            assertEquals(3, manager0.getElection().getVoteCount(),
                    "Vote count should be 3 after second failure");

            // Election should complete since we reached majority of 3
            assertEquals(NodeRole.CONTROLLER, manager0.getRole(),
                    "Manager 0 should be CONTROLLER after reaching majority via failed broker connections");

        } finally {
            for (Manager manager : managers) {
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

//    /**
//     * Tests that election completes when all broker connections fail.
//     * <p>
//     * Edge case scenario where all broker clients fail during an election.
//     * With all brokers failed, the candidate should still be able to win
//     * since each failed connection is treated as a granted vote.
//     * <p>
//     * Scenario:
//     * - 3 brokers total, initial majority = 2
//     * - All broker connections fail
//     * - Candidate wins with self-vote + 1 failed broker "vote"
//     */
//    @Test
//    @DisplayName("Election completes when all broker connections fail")
//    void electionCompletes_whenAllBrokerConnectionsFail() throws Exception {
//        final String debugPrefix = DEBUG_PREFIX + "[electionCompletes_whenAllBrokerConnectionsFail] ";
//
//        ExecutorService executor = Executors.newFixedThreadPool(4);
//        ArrayList<RaftManager> managers = new ArrayList<>();
//        TestControllerServer controllerServer = new TestControllerServer(8992);
//
//        try {
//            executor.execute(() -> Utils.checkedRunnableWrapper(controllerServer::start));
//            assertTrue(waitForController(controllerServer), "Max attempts reached. Failed to initialise controller server");
//
//            // Create 3 brokers
//            final int numBrokers = 3;
//
//            // Create node configs (brokers only)
//            List<NodeConfig> nodeConfigs = new ArrayList<>();
//            for (int i = 0; i < numBrokers; i++) {
//                nodeConfigs.add(new NodeConfig("" + i, new InetSocketAddress("localhost", 9000 + i)));
//            }
//
//            // Creating broker managers
//            for (int i = 0; i < numBrokers; i++) {
//                Path logPath = logsFolderPath.resolve(String.format("all-fail-%s.log", i));
//                WALogger logger = new WALogger(logPath);
//                RaftLogHandler logHandler = new RaftLogHandler(logger);
//
//                Path dir = snapshotFolderPath.resolve(String.format("all-fail-%s-snapshots", i));
//                Files.createDirectories(dir);
//                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
//                logHandler.setTerm(1L);
//                KVStore store = createStore(logHandler, snapshotter);
//
//                NodeConfig nodeConfig = nodeConfigs.get(i);
//                ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
//                nodeConfigsCopy.remove(nodeConfig);
//                RaftManager manager = new RaftManager(nodeConfig, store);
//                managers.add(manager);
//                final List<NodeConfig> finalConfigs = nodeConfigsCopy;
//                executor.submit(() -> {
//                    try {
//                        manager.start(finalConfigs);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//            }
//
//            // Waiting for all managers to startup
//            assertTrue(waitForManagers(managers), "Max attempts reached waiting for managers to start");
//
//            // Initiate election from manager 0
//            RaftManager manager0 = managers.get(0);
//            manager0.initiateElection();
//
//            // Verify initial election state
//            RaftManager.ElectionMeta meta = manager0.getElectionMeta();
//            assertNotNull(meta, "Election metadata should exist after initiation");
//            assertEquals(1L, meta.getTerm(), "Term should be 1 (0 + 1)");
//            assertEquals(1, meta.getVoteCount(), "Should start with self-vote");
//            assertEquals(2, meta.getMajority(), "Majority should be 2 with 2 running broker clients");
//
//            // Simulate all broker connections failing
//            // With 2 broker clients, both failing should give us 2 more votes
//            RequestVoteResponse failureVote1 = new RequestVoteResponse(true, 1L);
//            manager0.handleVoteResponse(failureVote1);
//
//            // After first failure, we should have reached majority
//            assertEquals(NodeRole.CONTROLLER, manager0.getRole(),
//                    "Manager 0 should be CONTROLLER after first broker failure (reached majority)");
//
//        } finally {
//            for (RaftManager manager : managers) {
//                manager.stop();
//            }
//
//            controllerServer.stop();
//            executor.shutdownNow();
//            boolean success = executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
//            if (!success) {
//                System.err.println(debugPrefix + "Executor termination failed");
//            }
//        }
//    }

    /**
     * Tests that vote responses with mismatched terms are ignored.
     * <p>
     * When a broker connection fails and the election term has changed,
     * the simulated vote response should not affect the current election.
     */
    @Test
    @DisplayName("Vote response with mismatched term is ignored during election")
    void voteResponseWithMismatchedTerm_isIgnored_duringElection() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[voteResponseWithMismatchedTerm_isIgnored_duringElection] ";

        ExecutorService executor = Executors.newFixedThreadPool(4);
        ArrayList<Manager> managers = new ArrayList<>();
        final int basePort = getBasePort();
        TestControllerServer controllerServer = new TestControllerServer(basePort + 1);

        try {
            executor.submit(() -> Utils.checkedRunnableWrapper(controllerServer::start));
            assertTrue(waitForController(controllerServer), "Max attempts reached. Failed to initialise controller server");

            // Create 3 brokers
            final int numBrokers = 3;

            // Create node configs (brokers only)
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            nodeConfigs.add(new NodeConfig("" + 0, new InetSocketAddress("localhost", basePort), new InetSocketAddress("localhost", basePort + 10)));
            nodeConfigs.add(new NodeConfig("controller", new InetSocketAddress(controllerServer.getHost(), controllerServer.getPort()), null));

            // Creating broker managers
//            for (int i = 0; i < numBrokers; i++) {
//                Path logPath = logsFolderPath.resolve(String.format("mismatched-term-%s.log", i));
//                WALogger logger = new WALogger(logPath);
//                RaftLogHandler logHandler = new RaftLogHandler(logger);
//
//                Path dir = snapshotFolderPath.resolve(String.format("mismatched-term-%s-snapshots", i));
//                Files.createDirectories(dir);
//                KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
//                logHandler.setTerm(1L);
//                KVStore store = createStore(logHandler, snapshotter);
//
//                NodeConfig nodeConfig = nodeConfigs.get(i);
//                ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
//                nodeConfigsCopy.remove(nodeConfig);
//                Manager manager = new Manager(store, nodeConfig, nodeConfigsCopy);
//                managers.add(manager);
//                manager.setLoggingEnabled(false);
//                final List<NodeConfig> finalConfigs = nodeConfigsCopy;
//                executor.submit(() -> {
//                    try {
//                        manager.start();
//                    } catch (IOException | InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//            }

            Path logPath = logsFolderPath.resolve(String.format("mismatched-term-%s.log", 0));
            WALogger logger = new WALogger(logPath);
            RaftLogHandler logHandler = new RaftLogHandler(logger);

            Path dir = snapshotFolderPath.resolve(String.format("mismatched-term-%s-snapshots", 0));
            Files.createDirectories(dir);
            KVMapSnapshotter snapshotter = new KVMapSnapshotter(dir);
            logHandler.setTerm(1L);
            KVStore store = createStore(logHandler, snapshotter);

            NodeConfig nodeConfig = nodeConfigs.get(0);
            ArrayList<NodeConfig> nodeConfigsCopy = new ArrayList<>(nodeConfigs);
            nodeConfigsCopy.remove(nodeConfig);
            Manager manager = new Manager(store, nodeConfig, nodeConfigsCopy);
            managers.add(manager);
//            manager.setLoggingEnabled(false);
            final List<NodeConfig> finalConfigs = nodeConfigsCopy;
            executor.submit(() -> {
                try {
                    manager.start();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            // Waiting for all managers to startup
            final long startTerm = managers.getFirst().getCurrentTerm();

            // Wait for manager's server to be running
            for (int i = 0; i < 5; i++) {
                if (manager.getServer() != null && manager.getServer().isRunning()) {
                    break;
                }
                Thread.sleep(500);
            }

            manager.setRole(NodeRole.CONTROLLER);
            Thread.sleep(1000);

            // Initiate election from manager 0
            Manager manager0 = managers.get(0);
//            manager0.setLoggingEnabled(false);
            manager0.startElection();

            // Verify initial election state
            Manager.Election meta = manager0.getElection();
            assertNotNull(meta, "Election metadata should exist after initiation");
            assertEquals(startTerm + 1, meta.getTerm(), "Term should be 1 (0 + 1)");
            assertEquals(1, meta.getVoteCount(), "Should start with self-vote");

            // Simulate a vote response with a mismatched term (old term)
            RequestVoteResponse mismatchedVote = new RequestVoteResponse(true, startTerm);
            manager0.handleVoteResponse(mismatchedVote);
            assertEquals(
                    1,
                    meta.getVoteCount(),
                    "Vote count should not change when receiving smaller term"
            );

            // Simulate a vote response with a future term - should be ignored
            RequestVoteResponse futureTermVote = new RequestVoteResponse(true, meta.getTerm() + 100);
            manager0.handleVoteResponse(futureTermVote);

            // Vote count should still remain unchanged
            assertEquals(1, meta.getVoteCount(),
                    "Vote count should not change for future term");

            // State should still be CANDIDATE
            assertEquals(NodeRole.CANDIDATE, manager0.getRole(),
                    "Should remain CANDIDATE after mismatched term votes");

        } finally {
            for (Manager manager : managers) {
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
     * Performs an exponential backoff waiting for the `controllerServer` to be running
     *
     * @param controllerServer
     * @return Whether the controller server is running
     * @throws InterruptedException
     */
    private boolean waitForController(TestControllerServer controllerServer) throws InterruptedException {
        final int maxAttempts = 5;

        for (int i = 0; i < maxAttempts; i++) {
            if (controllerServer.isRunning()) {
                return true;
            }
            Thread.sleep(500 * (i + 1));
        }

        return false;
    }

    /**
     * Performs an exponential backoff waiting for all managers to be running
     *
     * @param managers
     * @return Whether all managers are running
     * @throws InterruptedException
     */
    private boolean waitForManagers(List<RaftManager> managers) throws InterruptedException {
        final int maxAttempts = 5;

        for (int i = 0; i <= maxAttempts; i++) {
            boolean shouldBreak = true;

            for (RaftManager manager : managers) {
                shouldBreak = shouldBreak && manager.isRunning();
            }

            if (shouldBreak) {
                return true;
            }

            Thread.sleep(500 * (i + 1));
        }

        return false;
    }

    private boolean waitForManagersNew(List<Manager> managers) throws InterruptedException {
        final int maxAttempts = 5;

        for (int i = 0; i <= maxAttempts; i++) {
            boolean shouldBreak = true;

            for (Manager manager : managers) {
                ControllerClient controllerClient = manager.getControllerClient();
                shouldBreak = shouldBreak && (
                        manager.getRole() == NodeRole.CONTROLLER ||
                                controllerClient != null && controllerClient.getStatus() == ClientStatus.CONNECTED
                );
            }

            if (shouldBreak) {
                return true;
            }

            Thread.sleep(2000 * (i + 1));
        }

        return false;
    }

//    /**
//     * Heartbeat server as a replacement for a full RaftControllerServer
//     */
//    private class TestControllerServer {
//        private final String host = "localhost";
//        private final int port;
//        private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
//        private final String DBG_PREFIX = "[TestControllerServer]";
//        private volatile boolean running = false;
//        private Selector selector;
//        private ServerSocketChannel serverSocketChannel;
//
//        public TestControllerServer(int port) {
//            this.port = port;
//        }
//
//        /**
//         * Starts the server in its own thread.
//         * Safe to call multiple times.
//         */
//        public synchronized void start() throws IOException {
//            final String debugPrefix = DBG_PREFIX + "[start] ";
//
//            System.out.println(debugPrefix + "starting test controller");
//            if (running) {
//                return;
//            }
//
//            running = true;
//            selector = Selector.open();
//            serverSocketChannel = ServerSocketChannel.open();
//            serverSocketChannel.configureBlocking(false);
//            serverSocketChannel.bind(new InetSocketAddress(host, port));
//            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
//
//            while (running) {
//                int readyCount = selector.select(100);
//                if (readyCount == 0) continue;
//
//                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
//                while (iterator.hasNext()) {
//                    SelectionKey key = iterator.next();
//                    iterator.remove();
//
//                    if (!key.isValid()) continue;
//
//                    try {
//                        if (key.isAcceptable()) {
//                            handleAccept(key);
//                        } else if (key.isReadable()) {
//                            handleRead(key);
//                        } else if (key.isWritable()) {
//                            handleWrite(key);
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        cleanup(key);
//                    }
//                }
//            }
//
//            System.out.println(debugPrefix + "stopping");
//        }
//
//        /**
//         * Stops the server whether running in a thread or not.
//         */
//        public void stop() throws IOException {
//            final String debugPrefix = DBG_PREFIX + "[stop]";
//
//            if (!running) {
//                return;
//            }
//
//            running = false;
//
//            for (SelectionKey key : selector.keys()) {
//                try {
//                    cleanup(key);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
//
//            try {
//                selector.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//                throw e;
//            }
//
//            try {
//                serverSocketChannel.close();
//            } catch (Exception e) {
//                System.out.println(debugPrefix + "Exception during serverSocketChannel.close()");
//                e.printStackTrace();
//                throw e;
//            }
//        }
//
//        private void handleAccept(SelectionKey key) throws IOException {
//            ServerSocketChannel server = (ServerSocketChannel) key.channel();
//            SocketChannel client = server.accept();
//            if (client == null) {
//                return;
//            }
//
//            client.configureBlocking(false);
//            client.socket().setTcpNoDelay(true);
//            client.socket().setKeepAlive(true);
//
//            SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
//            clientKey.attach(new ClientSession(client));
//        }
//
//        private void handleRead(SelectionKey key) throws IOException {
//            SocketChannel channel = (SocketChannel) key.channel();
//            ClientSession session = (ClientSession) key.attachment();
//            ByteBuffer readBuffer = session.getReadBuffer();
//
//
//            int readCount = channel.read(readBuffer);
//
//            if (readCount == -1) {
//                cleanup(key);
//                return;
//            }
//
//            if (readCount == 0) {
//                return;
//            }
//
//            int position = readBuffer.position();
//            readBuffer.flip();
//            boolean processed = processData(session, readBuffer);
//            readBuffer.flip();
//
//            if (processed) {
//                readBuffer.compact();
//            } else {
//                readBuffer.position(position);
//            }
//        }
//
//        private boolean processData(ClientSession session, ByteBuffer buffer) {
//            final String debugPrefix = DBG_PREFIX + "[processData] ";
//
//
//            Message message;
//            try {
//                message = Message.deserialize(buffer);
//            } catch (IllegalArgumentException e) {
//                e.printStackTrace();
//                System.out.println(debugPrefix + " Exit method returning true (exception path)");
//                return true;
//            }
//
//            if (message == null) {
//                return false;
//            }
//
//            MessageType messageType = message.type();
//
//            ByteBuffer responseBuffer = null;
//
//            if (messageType.equals(MessageType.HEARTBEAT_REQUEST)) {
//                responseBuffer = ByteBuffer.wrap(new HeartbeatResponse().serialize());
//            }
//
//            if (responseBuffer != null) {
//                queueWrite(session.getChannel(), responseBuffer);
//            }
//
//            return true;
//        }
//
//        private void queueWrite(SocketChannel client, ByteBuffer data) {
//            Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(client, k -> new LinkedList<>());
//            queue.offer(data.duplicate());
//
//            SelectionKey key = client.keyFor(selector);
//            if (key != null && key.isValid()) {
//                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
//                selector.wakeup();
//            }
//        }
//
//        private void handleWrite(SelectionKey key) throws IOException {
//            SocketChannel client = (SocketChannel) key.channel();
//            Queue<ByteBuffer> queue = pendingWrites.get(client);
//
//            if (queue == null || queue.isEmpty()) {
//                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
//                return;
//            }
//
//            ByteBuffer buffer = queue.peek();
//            while (buffer != null) {
//                int bytesWritten = client.write(buffer);
//
//                if (bytesWritten == 0) {
//                    break;  // Send buffer full
//                }
//
//                if (!buffer.hasRemaining()) {
//                    queue.poll();
//                    buffer = queue.peek();
//                }
//            }
//
//            if (queue.isEmpty()) {
//                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
//            }
//        }
//
//        private void cleanup(SelectionKey key) {
//            if (key == null || !key.isValid()) return;
//
//            try {
//
//                SelectableChannel client = key.channel();
//                pendingWrites.remove(client);
//
//                key.cancel();
//                try {
//                    client.close();
//                } catch (IOException e) {
//                    // Ignore
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        public String getHost() {
//            return host;
//        }
//
//        public int getPort() {
//            return port;
//        }
//
//        public boolean isRunning() {
//            return serverSocketChannel != null && serverSocketChannel.isOpen();
//        }
//    }
}