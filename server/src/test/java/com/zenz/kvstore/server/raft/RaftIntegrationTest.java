package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.command.handler.RaftCommandHandler;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotBody;
import com.zenz.kvstore.server.snapshot.RaftSnapshotFooter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotHeader;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class RaftIntegrationTest {

    private Path logsDir;
    private Path snapshotsDir;

    @BeforeEach
    public void init() throws IOException {
        logsDir = Files.createTempDirectory("raft-integration-logs-");
        snapshotsDir = Files.createTempDirectory("raft-integration-snapshots-");
    }

    @AfterEach
    public void tearDown() throws IOException {
        logsDir.toFile().delete();
        snapshotsDir.toFile().delete();
    }

    @Test
    public void test_threeNodeCluster_electionAndLogReplication() throws IOException, InterruptedException {
        final int port1 = getRandomPort();
        final int port2 = getRandomPort();
        final int port3 = getRandomPort();

        final NodeConfig node1Config = new NodeConfig("node1", new InetSocketAddress("localhost", port1));
        final NodeConfig node2Config = new NodeConfig("node2", new InetSocketAddress("localhost", port2));
        final NodeConfig node3Config = new NodeConfig("node3", new InetSocketAddress("localhost", port3));

        final List<NodeConfig> allConfigs = List.of(node1Config, node2Config, node3Config);

        final Manager manager1 = createManager(node1Config, List.of(node2Config, node3Config));
        final Manager manager2 = createManager(node2Config, List.of(node1Config, node3Config));
        final Manager manager3 = createManager(node3Config, List.of(node1Config, node2Config));

        final ExecutorService executor = Executors.newFixedThreadPool(3);

        try (manager1; manager2; manager3) {
            executor.submit(runnableWrapper(manager1::open));
            assertTrue(awaitWakeUpManager(manager1), "Manager 1 failed to start");

            executor.submit(runnableWrapper(manager2::open));
            assertTrue(awaitWakeUpManager(manager2), "Manager 2 failed to start");

            executor.submit(runnableWrapper(manager3::open));
            assertTrue(awaitWakeUpManager(manager3), "Manager 3 failed to start");

            log.info("Waiting for initial election...");
            Thread.sleep(8_000);

            final State state1 = manager1.getStateObject().getState();
            final State state2 = manager2.getStateObject().getState();
            final State state3 = manager3.getStateObject().getState();

            log.info("States after initial election - node1: {}, node2: {}, node3: {}", state1, state2, state3);

            final long leaderCount = Stream.of(state1, state2, state3)
                    .filter(s -> s == State.LEADER)
                    .count();
            assertEquals(1, leaderCount, "Exactly one node should be leader after initial election");

            Manager leaderManager = null;
            List<Manager> followers = new ArrayList<>();
            if (state1 == State.LEADER) {
                leaderManager = manager1;
                followers.add(manager2);
                followers.add(manager3);
            } else if (state2 == State.LEADER) {
                leaderManager = manager2;
                followers.add(manager1);
                followers.add(manager3);
            } else {
                leaderManager = manager3;
                followers.add(manager1);
                followers.add(manager2);
            }

            assertNotNull(leaderManager, "Should have identified a leader");
            assertNotNull(leaderManager.getNodeConfig(), "Leader should have config");

            final RaftCommandHandler commandHandler = new RaftCommandHandler(leaderManager);
            commandHandler.handleCommand(new PutCommand("key1", "value1".getBytes(StandardCharsets.UTF_8)));
            commandHandler.handleCommand(new PutCommand("key2", "value2".getBytes(StandardCharsets.UTF_8)));
            commandHandler.handleCommand(new PutCommand("key3", "value3".getBytes(StandardCharsets.UTF_8)));

            log.info("Leader {} added 3 entries. Waiting for replication...", leaderManager.getNodeConfig().id());
            Thread.sleep(10_000);

            final RaftLogHandler leaderLogHandler = (RaftLogHandler) leaderManager.getStateObject().getLogHandler();
            final int leaderLogId = (int) leaderLogHandler.getLogId();
            log.info("Leader log ID: {}", leaderLogId);

            for (Manager follower : followers) {
                final RaftLogHandler followerLogHandler = (RaftLogHandler) follower.getStateObject().getLogHandler();
                log.info("Follower {} log ID: {}", follower.getNodeConfig().id(), followerLogHandler.getLogId());

                assertEquals(leaderLogId, followerLogHandler.getLogId(),
                        "Follower " + follower.getNodeConfig().id() + " should have same log ID as leader");
            }

            log.info("Stopping leader: {}", leaderManager.getNodeConfig().id());
            leaderManager.close();

            Thread.sleep(8_000);

            final List<Manager> managers = new ArrayList<>();
            if (!manager1.getNodeConfig().id().equals(leaderManager.getNodeConfig().id())) {
                managers.add(manager1);
            }
            if (!manager2.getNodeConfig().id().equals(leaderManager.getNodeConfig().id())) {
                managers.add(manager2);
            }
            if (!manager3.getNodeConfig().id().equals(leaderManager.getNodeConfig().id())) {
                managers.add(manager3);
            }
            final Manager remainingManger1 = managers.getFirst();
            final Manager remainingManger2 = managers.getLast();
            final State newState1 = remainingManger1.getStateObject().getState();
            final State newState2 = remainingManger2.getStateObject().getState();
            log.info(
                    "States after leader failure - {}: {}, {}: {}",
                    remainingManger1.getNodeConfig().id(),
                    newState1,
                    remainingManger2.getNodeConfig().id(),
                    newState2);

            final long newLeaderCount = managers.stream()
                    .filter(m -> m.getStateObject().getState() == State.LEADER)
                    .count();
            assertEquals(
                    1,
                    newLeaderCount,
                    "Exactly one node should be leader after original leader fails");

            if (newState1 == State.LEADER) {
                assertEquals(remainingManger1.getNodeConfig().id(), remainingManger2.getStateObject().getLeaderId());
            } else {
                assertEquals(remainingManger2.getNodeConfig().id(), remainingManger1.getStateObject().getLeaderId());
            }

            Manager newLeader = newState2 == State.LEADER ? manager2 : manager3;
            Manager remainingFollower = newState2 == State.LEADER ? manager3 : manager2;

            assertNotNull(newLeader, "Should have a new leader");
            assertNotNull(remainingFollower, "Should have one remaining follower");

            commandHandler.handleCommand(new PutCommand("key4", "value4".getBytes(StandardCharsets.UTF_8)));
            log.info("New leader {} added key4. Waiting for replication...", newLeader.getNodeConfig().id());
            Thread.sleep(5_000);

            final RaftLogHandler newLeaderLogHandler = newLeader.getStateObject().getLogHandler();
            final RaftLogHandler remainingFollowerLogHandler = remainingFollower.getStateObject().getLogHandler();

            assertEquals(newLeaderLogHandler.getLogId(), remainingFollowerLogHandler.getLogId(),
                    "Remaining follower should have same log ID as new leader");
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private int getRandomPort() {
        return new Random().nextInt(10000, 60000);
    }

    private Manager createManager(final NodeConfig nodeConfig, final List<NodeConfig> peerConfigs) throws IOException {
        final var snapshotter = new KVStoreSnapshotter<>(
                RaftSnapshotHeader.class,
                RaftSnapshotBody.class,
                RaftSnapshotFooter.class);
        final var snapshotsDir = this.snapshotsDir.resolve(nodeConfig.id() + "-snapshots");
        Files.createDirectories(snapshotsDir);

        final Manager manager = new Manager(
                new KVStore(new RaftLogHandler(
                        new WALogger(logsDir.resolve(nodeConfig.id() + ".log")),
                        snapshotter)),
                nodeConfig,
                peerConfigs);
        final ClientObserver clientObserver = new TestClientObserver();
        final ServerObserver serverObserver = new TestServerObserver();

        manager.setClientObserver(clientObserver);
        manager.setServerObserver(serverObserver);

        return manager;
    }

    private boolean awaitWakeUpManager(final Manager manager) throws InterruptedException {
        for (int i = 0; i < 15; i++) {
            if (manager.isOpen()) {
                return true;
            }
            Thread.sleep(1000);
        }
        return false;
    }

    private Runnable runnableWrapper(final RunnableEx handler) {
        return () -> {
            try {
                handler.run();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @FunctionalInterface
    private interface RunnableEx {
        void run() throws Exception;
    }
}