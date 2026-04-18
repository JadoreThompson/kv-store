package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.raft.message.*;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class ElectionTest {

    private Path logsDir;
    private Path snapshotDir;
    private NodeConfig managerConfig;

    @BeforeEach
    public void init() throws IOException {
        logsDir = Files.createTempDirectory("raft-test-logs-");
        snapshotDir = Files.createTempDirectory("test-snapshots-");
        managerConfig = new NodeConfig("test-manager", new InetSocketAddress("localhost", 9999));
    }

    @AfterEach
    public void tearDown() throws IOException {
        logsDir.toFile().delete();
        snapshotDir.toFile().delete();
    }

    @Test
    public void test_electionTimeout_electionConcludes() throws IOException, InterruptedException {
        final NodeConfig peer = new NodeConfig("peer", new InetSocketAddress("localhost", getRandomPort()));
        final TestRaftServer peerServer = new TestRaftServer(peer.address());

        final List<Message> messages = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(2);

        peerServer.setMessageHandler((message) -> {
            synchronized (messages) {
                messages.add(message);
            }
            latch.countDown();

            if (message.type() == RaftMessageType.REQUEST_VOTE) {
                return ByteBuffer.wrap(new RequestVoteResponse(
                        ((RequestVote) message).term(), true).serialize());
            }
            return null;
        });

        final Manager manager = createManager(managerConfig, List.of(peer));
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try (manager; peerServer) {
            executor.submit(runnableWrapper(peerServer::open));
            boolean awake = awaitWakeUpTestRaftServer(peerServer);
            assertTrue(awake, "Peer server failed to start");

            executor.submit(runnableWrapper(manager::open));
            awake = awaitWakeUpManager(manager);
            assertTrue(awake, "Manager failed to start");

            assertEquals(
                    State.FOLLOWER,
                    manager.getStateObject().state,
                    "Node state should be initialised to FOLLOWER"
            );

            boolean completed = latch.await(5, TimeUnit.SECONDS);
            assertTrue(completed, "Timed out waiting for 2 messages. Got: " + messages.size());
            synchronized (messages) {
                assertEquals(
                        RaftMessageType.REQUEST_VOTE,
                        messages.getFirst().type(),
                        "First message should be a vote request"
                );
                assertEquals(
                        RaftMessageType.APPEND_ENTRY,
                        messages.get(1).type(),
                        "Second message should be an append entry after winning the election"
                );
            }
        }
    }

    @Test
    public void test_nodeWithLargerLogId_winsElection() throws IOException, InterruptedException {
        final NodeConfig smallerNodeConfig = new NodeConfig(
                "smaller",
                new InetSocketAddress("localhost", getRandomPort()));
        final NodeConfig largerNodeConfig = new NodeConfig(
                "larger",
                new InetSocketAddress("localhost", getRandomPort()));

        final Manager smallerManager = createManager(smallerNodeConfig, List.of(largerNodeConfig));
        final Manager largerManager = createManager(largerNodeConfig, List.of(smallerNodeConfig));
        final KVStore largerKvstore = largerManager.getKvstore();
        largerKvstore.put("key", "value".getBytes(StandardCharsets.UTF_8));

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try (smallerManager; largerManager) {
            executor.submit(runnableWrapper(smallerManager::open));
            boolean awake = awaitWakeUpManager(smallerManager);
            assertTrue(awake, "Server failed to start");

            executor.submit(runnableWrapper(largerManager::open));
            awake = awaitWakeUpManager(largerManager);
            assertTrue(awake, "Server failed to start");

            Thread.sleep(10_000);

            assertEquals(
                    State.LEADER,
                    largerManager.getStateObject().state,
                    String.format(
                            "Smaller state: %s, larger state: %s",
                            smallerManager.getStateObject().state,
                            largerManager.getStateObject().state));
            assertEquals(State.FOLLOWER, smallerManager.getStateObject().state);

            final TestServerObserver smallerServerObserver = (TestServerObserver) smallerManager.getServerObserver();
            assertInstanceOf(AppendEntry.class, smallerServerObserver.receivedMessages.getLast());
            assertInstanceOf(AppendEntryResponse.class, smallerServerObserver.sentMessages.getLast());
        }
    }

    @Test
    public void test_splitVote_bothCandidatesNoMajority() throws IOException, InterruptedException {
        final NodeConfig nodeAConfig = new NodeConfig(
                "nodeA",
                new InetSocketAddress("localhost", getRandomPort()));
        final NodeConfig nodeBConfig = new NodeConfig(
                "nodeB",
                new InetSocketAddress("localhost", getRandomPort()));

        final Manager nodeAManager = createManager(nodeAConfig, List.of(nodeBConfig));
        final Manager nodeBManager = createManager(nodeBConfig, List.of(nodeAConfig));

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try (nodeAManager; nodeBManager) {
            executor.submit(runnableWrapper(nodeAManager::open));
            assertTrue(awaitWakeUpManager(nodeAManager), "Node A failed to start");

            executor.submit(runnableWrapper(nodeBManager::open));
            assertTrue(awaitWakeUpManager(nodeBManager), "Node B failed to start");

            Thread.sleep(10_000);

            final State stateA = nodeAManager.getStateObject().state;
            final State stateB = nodeBManager.getStateObject().state;

            // NOTE: Wrong
            assertFalse(
                    stateA == State.LEADER && stateB == State.LEADER,
                    "Both nodes should not become leaders (split vote)");
        }
    }

    @Test
    public void test_threeNodes_majorityWinsElection() throws IOException, InterruptedException {
        final NodeConfig node1Config = new NodeConfig(
                "node1",
                new InetSocketAddress("localhost", getRandomPort()));
        final NodeConfig node2Config = new NodeConfig(
                "node2",
                new InetSocketAddress("localhost", getRandomPort()));
        final NodeConfig node3Config = new NodeConfig(
                "node3",
                new InetSocketAddress("localhost", getRandomPort()));

        final Manager node1Manager = createManager(node1Config, List.of(node2Config, node3Config));
        final Manager node2Manager = createManager(node2Config, List.of(node1Config, node3Config));
        final Manager node3Manager = createManager(node3Config, List.of(node1Config, node2Config));

        final ExecutorService executor = Executors.newFixedThreadPool(3);

        try (node1Manager; node2Manager; node3Manager) {
            executor.submit(runnableWrapper(node1Manager::open));
            assertTrue(awaitWakeUpManager(node1Manager), "Node 1 failed to start");

            executor.submit(runnableWrapper(node2Manager::open));
            assertTrue(awaitWakeUpManager(node2Manager), "Node 2 failed to start");

            executor.submit(runnableWrapper(node3Manager::open));
            assertTrue(awaitWakeUpManager(node3Manager), "Node 3 failed to start");

            Thread.sleep(10_000);

            final State state1 = node1Manager.getStateObject().state;
            final State state2 = node2Manager.getStateObject().state;
            final State state3 = node3Manager.getStateObject().state;

            final long leaderCount = Stream.of(new State[]{state1, state2, state3})
                    .filter(s -> s == State.LEADER)
                    .count();

            assertEquals(1, leaderCount, "Exactly one leader should be elected");
        }
    }

    @Test
    public void test_lowerLogNode_cannotWinElection() throws IOException, InterruptedException {
        final NodeConfig leaderConfig = new NodeConfig(
                "leader",
                new InetSocketAddress("localhost", getRandomPort()));
        final NodeConfig followerConfig = new NodeConfig(
                "follower",
                new InetSocketAddress("localhost", getRandomPort()));

        final KVStore leaderKvstore = new KVStore(new RaftLogHandler(
                new WALogger(logsDir.resolve(leaderConfig.id() + "'\'app.log")),
                new KVStoreSnapshotter<>(
                        RaftSnapshotHeader.class,
                        RaftSnapshotBody.class,
                        RaftSnapshotFooter.class)));
        leaderKvstore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        leaderKvstore.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        final KVStore followerKvstore = new KVStore(new RaftLogHandler(
                new WALogger(logsDir.resolve(followerConfig.id() + "'\'app.log")),
                new KVStoreSnapshotter<>(
                        RaftSnapshotHeader.class,
                        RaftSnapshotBody.class,
                        RaftSnapshotFooter.class)));
        followerKvstore.put("key1", "value1".getBytes(StandardCharsets.UTF_8));

        final NodeConfig actualLeaderConfig = leaderConfig;
        final NodeConfig actualFollowerConfig = followerConfig;

        final Manager leaderManager = createManagerWithKvstore(leaderKvstore, actualLeaderConfig, List.of(actualFollowerConfig));
        final Manager followerManager = createManagerWithKvstore(followerKvstore, actualFollowerConfig, List.of(actualLeaderConfig));

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try (leaderManager; followerManager) {
            executor.submit(runnableWrapper(leaderManager::open));
            assertTrue(awaitWakeUpManager(leaderManager), "Leader node failed to start");

            executor.submit(runnableWrapper(followerManager::open));
            assertTrue(awaitWakeUpManager(followerManager), "Follower node failed to start");

            Thread.sleep(10_000);

            assertEquals(
                    State.LEADER,
                    leaderManager.getStateObject().state,
                    "Node with more log entries should become leader");
            assertEquals(
                    State.FOLLOWER,
                    followerManager.getStateObject().state,
                    "Node with fewer log entries should become follower");
        }
    }

    @Test
    public void test_sameLogId_differentTerms_higherTermWins() throws IOException, InterruptedException {
        final NodeConfig nodeLowerConfig = new NodeConfig(
                "lowerTerm",
                new InetSocketAddress("localhost", getRandomPort()));
        final NodeConfig nodeHigherConfig = new NodeConfig(
                "higherTerm",
                new InetSocketAddress("localhost", getRandomPort()));

        final RaftLogHandler lowerTermLogHandler = new RaftLogHandler(
                new WALogger(logsDir.resolve(nodeLowerConfig.id() + "'\'app.log")),
                new KVStoreSnapshotter<>(
                        RaftSnapshotHeader.class,
                        RaftSnapshotBody.class,
                        RaftSnapshotFooter.class));
        lowerTermLogHandler.setTerm(1L);
        final KVStore lowerTermKvstore = new KVStore(lowerTermLogHandler);
        lowerTermKvstore.put("key", "value".getBytes(StandardCharsets.UTF_8));

        final RaftLogHandler higherTermLogHandler = new RaftLogHandler(
                new WALogger(logsDir.resolve(nodeHigherConfig.id() + "'\'app.log")),
                new KVStoreSnapshotter<>(
                        RaftSnapshotHeader.class,
                        RaftSnapshotBody.class,
                        RaftSnapshotFooter.class));
        higherTermLogHandler.setTerm(2L);
        final KVStore higherTermKvstore = new KVStore(higherTermLogHandler);
        higherTermKvstore.put("key", "value".getBytes(StandardCharsets.UTF_8));

        final Manager lowerTermManager = createManagerWithKvstore(lowerTermKvstore, nodeLowerConfig, List.of(nodeHigherConfig));
        final Manager higherTermManager = createManagerWithKvstore(higherTermKvstore, nodeHigherConfig, List.of(nodeLowerConfig));

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try (lowerTermManager; higherTermManager) {
            executor.submit(runnableWrapper(lowerTermManager::open));
            assertTrue(awaitWakeUpManager(lowerTermManager), "Lower term node failed to start");

            executor.submit(runnableWrapper(higherTermManager::open));
            assertTrue(awaitWakeUpManager(higherTermManager), "Higher term node failed to start");

            Thread.sleep(10_000);

            final State stateLower = lowerTermManager.getStateObject().state;
            final State stateHigher = higherTermManager.getStateObject().state;

            assertEquals(
                    State.LEADER,
                    stateHigher,
                    "Node with higher term should become leader");
            assertEquals(
                    State.FOLLOWER,
                    stateLower,
                    "Node with lower term should become follower");
        }
    }

    private int getRandomPort() {
        return new Random().nextInt(1000, 9999);
    }

    private Manager createManager(final NodeConfig nodeConfig, final List<NodeConfig> peerConfigs) throws IOException {
        final Manager manager = new Manager(
                new KVStore(new RaftLogHandler(
                        new WALogger(logsDir.resolve(nodeConfig.id() + "'\'app.log")),
                        new KVStoreSnapshotter<>(
                                RaftSnapshotHeader.class,
                                RaftSnapshotBody.class,
                                RaftSnapshotFooter.class))),
                nodeConfig,
                peerConfigs);

        final ClientObserver clientObserver = new TestClientObserver();
        final ServerObserver serverObserver = new TestServerObserver();

        manager.setClientObserver(clientObserver);
        manager.setServerObserver(serverObserver);

        return manager;
    }

    private boolean awaitWakeUp(final Supplier<Boolean> predicate) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            if (predicate.get()) {
                return true;
            }

            Thread.sleep(1000);
        }

        return false;
    }

    private boolean awaitWakeUpTestRaftServer(final TestRaftServer testRaftServer) throws InterruptedException {
        return awaitWakeUp(testRaftServer::isRunning);
    }

    private boolean awaitWakeUpManager(final Manager testRaftServer) throws InterruptedException {
        return awaitWakeUp(testRaftServer::isOpen);
    }

    private Runnable runnableWrapper(final CheckedRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private interface CheckedRunnable {

        void run() throws IOException, InterruptedException;
    }

    private Manager createManagerWithKvstore(
            final KVStore kvStore,
            final NodeConfig nodeConfig,
            final List<NodeConfig> peerConfigs) {
        final Manager manager = new Manager(
                kvStore,
                nodeConfig,
                peerConfigs);

        final ClientObserver clientObserver = new TestClientObserver();
        final ServerObserver serverObserver = new TestServerObserver();

        manager.setClientObserver(clientObserver);
        manager.setServerObserver(serverObserver);

        return manager;
    }
}
