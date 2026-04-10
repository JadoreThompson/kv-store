package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.RaftLogHandler;
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

    private Path logsDir;

    private Path snapshotsDir;

    @BeforeEach
    void beforeEach() throws IOException {
        this.tempDir = Files.createTempDirectory("leader-election-test-");
        this.logsDir = this.tempDir.resolve("logs");
        this.snapshotsDir = this.tempDir.resolve("snapshots");

        Files.createDirectories(this.logsDir);
        Files.createDirectories(this.snapshotsDir);
    }

    @AfterEach
    void afterEach() {
        this.tempDir.toFile().delete();
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

    private int getBasePort() {
        final Random random = new Random();
        return random.nextInt(1000, 9994);
    }

    private boolean awaitBootstrapCompletion(List<Manager> managers) throws InterruptedException {
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

    private void shutdownAll(final List<Manager> managers, final ExecutorService executorService) throws InterruptedException, IOException {
        for (Manager manager : managers) {
            manager.stop();
        }

        executorService.shutdown();
        final boolean success = executorService.awaitTermination(5, TimeUnit.SECONDS);
        if (!success) {
            throw new RuntimeException("Executor service failed to terminate");
        }
    }

    private void startManager(final Manager manager) {
        try {
            manager.start();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ManagerContext {

        public final Path logPath;
        public final Path snapshotsDir;
        public final KVStore kvStore;
        public final Manager manager;

        public ManagerContext(
                final String name,
                final Path logsDir,
                final Path snapshotsDir,
                final NodeConfig nodeConfig,
                final List<NodeConfig> nodeConfigs
        ) throws IOException {
            this.logPath = logsDir.resolve("manager-" + name + ".log");
            final WALogger logger = new WALogger(this.logPath);
            final RaftLogHandler logHandler = new RaftLogHandler(logger);

            this.snapshotsDir = snapshotsDir.resolve("manager-" + name + "-snapshots");
            Files.createDirectories(this.snapshotsDir);
            final KVMapSnapshotter snapshotter = new KVMapSnapshotter(this.snapshotsDir);

            this.kvStore = new KVStore(
                    new KVStore.Builder()
                            .setLogHandler(logHandler)
                            .setSnapshotter(snapshotter)
                            .setRaftMode(true)
            );
            this.manager = new Manager(this.kvStore, nodeConfig, nodeConfigs);
        }
    }

    @Test
    @DisplayName("Tests that bootstrapping concludes with fresh nodes")
    void testBootstrapConcludes_withFreshNodes() throws IOException, InterruptedException {
        final int numNodes = 3;
        final ExecutorService executorService = Executors.newFixedThreadPool(numNodes);
        final List<Manager> managers = new ArrayList<>();
        final List<NodeConfig> nodeConfigs = new ArrayList<>();

        final int basePort = getBasePort();
        for (int i = 0; i < numNodes; i++) {
            nodeConfigs.add(new NodeConfig("Node-" + i, new InetSocketAddress("localhost", basePort + i), null));
        }

        try {
            for (int i = 0; i < numNodes; i++) {
                final NodeConfig nodeConfig = nodeConfigs.get(i);
                final ManagerContext managerContext = new ManagerContext(
                        "" + i,
                        this.logsDir,
                        this.snapshotsDir,
                        nodeConfig,
                        nodeConfigs.stream().filter(config -> config != nodeConfig).toList());
                managers.add(managerContext.manager);
                managerContext.manager.setLoggingEnabled(false);
                executorService.submit(() -> startManager(managerContext.manager));
            }

            awaitBootstrapCompletion(managers);
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
            shutdownAll(managers, executorService);
        }
    }

    @Test
    @DisplayName("Complete election cycle from initiation to completion")
    void completeElectionCycle_fromInitiationToCompletion_with3Nodes() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[completeElectionCycle_fromInitiationToCompletion_with3Nodes] ";

        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        final ArrayList<Manager> managers = new ArrayList<>();

        try {
            // Creating nodes
            final List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < 3; i++) {
                nodeConfigs.add(
                        new NodeConfig(
                                "" + i,
                                new InetSocketAddress("localhost", basePort + i),
                                null));
            }

            // Creating managers
            for (int i = 0; i < nodeConfigs.size(); i++) {
                final NodeConfig nodeConfig = nodeConfigs.get(i);
                final ManagerContext managerContext = new ManagerContext(
                        "" + i,
                        this.logsDir,
                        this.snapshotsDir,
                        nodeConfig,
                        nodeConfigs.stream().filter(config -> config != nodeConfig).toList());

                // Ensuring first manager wins the bootstrapping process
                ((RaftLogHandler) managerContext.kvStore.getLogHandler()).setTerm(1L);
                managerContext.kvStore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
                if (i == 0) {
                    managerContext.kvStore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
                    managerContext.kvStore.put("key3", "value3".getBytes(StandardCharsets.UTF_8));
                }

                managerContext.manager.setLoggingEnabled(false);
                managers.add(managerContext.manager);
                executorService.submit(() -> startManager(managerContext.manager));
            }

            assertTrue(awaitBootstrapCompletion(managers), "Max attempts reached. Managers failed to initialise");

            final Manager manager0 = managers.getFirst();
            manager0.startElection();
            Thread.sleep(3_000);

            // Assertions
            assertEquals(NodeRole.CONTROLLER, manager0.getRole(), "Manager 0 should now be CONTROLLER");
            assertEquals(NodeRole.BROKER, managers.get(1).getRole(), "Manager 1 should be FOLLOWER");
            assertEquals(NodeRole.BROKER, managers.get(2).getRole(), "Manager 2 should be FOLLOWER");
            assertTrue(manager0.getServer().isRunning(), "Server must be running");

            final InetSocketAddress address0 = manager0.getNodeConfig().serverAddress();
            final ControllerClient controllerClient2 = managers.get(2).getControllerClient();
            assertNotNull(
                    controllerClient2,
                    "Manager 2 controller should have a controller client connected to manager 0");
            assertEquals(
                    address0,
                    controllerClient2.getRemoteAddress(),
                    "Manager 2 should have a controller client connection to Manager 0 as it's controller"
            );
            final ControllerClient controllerClient1 = managers.get(1).getControllerClient();
            assertNotNull(
                    controllerClient1,
                    "Manager 1 controller should have a controller client connected to manager 0");
            assertEquals(
                    address0,
                    controllerClient1.getRemoteAddress(),
                    "Manager 1 should have a controller client connection to Manager 0 as it's controller"
            );
        } finally {
            shutdownAll(managers, executorService);
        }
    }

    @Test
    @DisplayName("Election succeeds despite initial vote denial")
    void electionSucceeds_despiteInitialVoteDenial() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[electionSucceeds_despiteInitialVoteDenial] ";
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final ArrayList<Manager> managers = new ArrayList<>();

        try {
            // Create node configs
            final List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < 3; i++) {
                nodeConfigs.add(new NodeConfig(
                        "" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

            // Creating managers
            for (int i = 0; i < nodeConfigs.size(); i++) {
                final NodeConfig nodeConfig = nodeConfigs.get(i);
                final ManagerContext managerContext = new ManagerContext(
                        "" + i,
                        this.logsDir,
                        this.snapshotsDir,
                        nodeConfig,
                        nodeConfigs.stream().filter(config -> config != nodeConfig).toList());

                final RaftLogHandler logHandler = (RaftLogHandler) managerContext.kvStore.getLogHandler();
                logHandler.setTerm(3L);
                logHandler.setLogId(10L);

                managerContext.manager.setLoggingEnabled(false);
                managers.add(managerContext.manager);
                executor.submit(() -> startManager(managerContext.manager));
            }

            assertTrue(awaitBootstrapCompletion(managers), "Max attempts reached waiting for managers to start");

            // Initiating election with the first manager
            Manager manager0 = managers.getFirst();
            manager0.startElection();

            // Verify initial election state with 2 running broker clients, majority should be 2
            Manager.Election election = manager0.getElection();
            assertNotNull(election, "Election should exist after initiation");
            assertEquals(1, election.getVoteCount(), "Should start with self-vote");
            assertEquals(2, election.getMajority(), "Majority should be 2 (2 running brokers / 2 + 1)");

            // Simulate denied vote (broker already voted for someone else)
            RequestVoteResponse deniedVote = new RequestVoteResponse(false, election.getTerm());
            manager0.handleVoteResponse(deniedVote);

            // Vote count unchanged, election still in progress
            assertEquals(1, manager0.getElection().getVoteCount(),
                    "Denied vote should not change vote count");
            assertEquals(NodeRole.CANDIDATE, manager0.getRole(),
                    "Should remain CANDIDATE after denied vote");

            // This should reach majority (2 votes out of 2 needed)
            RequestVoteResponse grantedVote = new RequestVoteResponse(true, election.getTerm());
            manager0.handleVoteResponse(grantedVote);

            assertEquals(
                    NodeRole.CONTROLLER,
                    manager0.getRole(),
                    "Manager 0 should now be elected leader with 2 out of 2 needed votes to satisfy the majority."
            );

        } finally {
            shutdownAll(managers, executor);
        }
    }

    @Test
    @DisplayName("Broker connection failure increments vote count during election")
    void brokerConnectionFailure_incrementsVoteCount_duringElection() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[brokerConnectionFailure_incrementsVoteCount_duringElection] ";

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final ArrayList<Manager> managers = new ArrayList<>();

        try {
            // Create node configs
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < 3; i++) {
                nodeConfigs.add(new NodeConfig(
                        "" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

            // Creating managers
            for (int i = 0; i < 3; i++) {
                final NodeConfig nodeConfig = nodeConfigs.get(i);
                final ManagerContext managerContext = new ManagerContext(
                        "" + i,
                        this.logsDir,
                        this.snapshotsDir,
                        nodeConfig,
                        nodeConfigs.stream().filter(config -> config != nodeConfig).toList());

                ((RaftLogHandler) managerContext.kvStore.getLogHandler()).setTerm(1L);
                managerContext.manager.setLoggingEnabled(false);
                managers.add(managerContext.manager);
                executor.submit(() -> startManager(managerContext.manager));
            }

            // Waiting for all managers to startup
            assertTrue(awaitBootstrapCompletion(managers), "Max attempts reached waiting for managers to start");
            final long term = managers.getFirst().getCurrentTerm();

            // Initiate election from manager 0
            Manager manager0 = managers.getFirst();
            manager0.startElection();

            // Verify initial election state
            Manager.Election election = manager0.getElection();
            assertNotNull(election, "Election should exist after initiation");
            assertEquals(term + 1, election.getTerm(), "Term should be term+1");
            assertEquals(1, election.getVoteCount(), "Should start with self-vote");
            assertEquals(2, election.getMajority(), "Majority should be 2 with 2 running broker clients");

            // Simulate broker connection failure by directly calling handleVoteResponse
            // This simulates what happens in RaftBrokerClient.handleRead() when bytesRead == -1
            RequestVoteResponse simulatedFailureVote = new RequestVoteResponse(true, election.getTerm());
            manager0.handleVoteResponse(simulatedFailureVote);

            // Vote count should now be 2 (self-vote + failed broker treated as granted)
            assertEquals(2, manager0.getElection().getVoteCount(),
                    "Vote count should increment when broker connection fails");

            // Election should complete since we reached majority
            assertEquals(NodeRole.CONTROLLER, manager0.getRole(),
                    "Manager 0 should be CONTROLLER after reaching majority via failed broker connection");
        } finally {
            shutdownAll(managers, executor);
        }
    }

    @Test
    @DisplayName("Election completes with multiple broker connection failures")
    void electionCompletes_withMultipleBrokerConnectionFailures() throws Exception {
        final String debugPrefix = DEBUG_PREFIX + "[electionCompletes_withMultipleBrokerConnectionFailures] ";

        final ExecutorService executorService = Executors.newFixedThreadPool(8);
        final ArrayList<Manager> managers = new ArrayList<>();

        try {
            // Create node configs
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            final int basePort = getBasePort();
            for (int i = 0; i < 5; i++) {
                nodeConfigs.add(new NodeConfig(
                        "" + i, new InetSocketAddress("localhost", basePort + i), null));
            }

            // Creating managers
            for (int i = 0; i < nodeConfigs.size(); i++) {
                final NodeConfig nodeConfig = nodeConfigs.get(i);
                final ManagerContext managerContext = new ManagerContext(
                        nodeConfig.name(),
                        this.logsDir,
                        this.snapshotsDir,
                        nodeConfig,
                        nodeConfigs.stream().filter(config -> config != nodeConfig).toList());

                ((RaftLogHandler) managerContext.kvStore.getLogHandler()).setTerm(1L);
                managerContext.manager.setLoggingEnabled(false);
                managers.add(managerContext.manager);
                executorService.submit(() -> startManager(managerContext.manager));
            }

            // Waiting for all managers to startup
            assertTrue(awaitBootstrapCompletion(managers), "Max attempts reached waiting for managers to start");
            final long startTerm = managers.getFirst().getCurrentTerm();

            // Initiate election from manager 0
            Manager manager0 = managers.getFirst();
            manager0.startElection();

            // Verify initial election state
            // With 5 brokers, we have 4 broker clients + self = 5 total
            // Majority = 5 / 2 + 1 = 3
            Manager.Election election = manager0.getElection();
            assertNotNull(election, "Election should exist after initiation");
            assertEquals(startTerm + 1, election.getTerm(), "Term should be 1 (0 + 1)");
            assertEquals(1, election.getVoteCount(), "Should start with self-vote");
            assertEquals(
                    3,
                    election.getMajority(),
                    "Majority should be 3 with 4 running broker clients + self");

            // Simulate first broker connection failure
            RequestVoteResponse failureVote1 = new RequestVoteResponse(true, election.getTerm());
            manager0.handleVoteResponse(failureVote1);

            assertEquals(2, manager0.getElection().getVoteCount(),
                    "Vote count should be 2 after first failure");

            // Simulate second broker connection failure
            RequestVoteResponse failureVote2 = new RequestVoteResponse(true, election.getTerm());
            manager0.handleVoteResponse(failureVote2);

            assertEquals(3, manager0.getElection().getVoteCount(),
                    "Vote count should be 3 after second failure");

            // Election should complete since we reached the majority of 3
            assertEquals(NodeRole.CONTROLLER, manager0.getRole(),
                    "Manager 0 should be CONTROLLER after reaching majority via failed broker connections");

        } finally {
            shutdownAll(managers, executorService);
        }
    }
}