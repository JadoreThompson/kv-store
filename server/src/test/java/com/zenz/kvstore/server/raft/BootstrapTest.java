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
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class BootstrapTest {

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

    private static class TestClientObserver implements ClientObserver {

        public final Map<InetSocketAddress, List<Message>> sentMessages = new HashMap<>();
        public final Map<InetSocketAddress, List<Message>> receivedMessages = new HashMap<>();

        @Override
        public void onSend(final InetSocketAddress to, final Message message) {
            sentMessages.computeIfAbsent(to, k -> new ArrayList<>()).add(message);
        }

        @Override
        public void onReceive(final InetSocketAddress from, final Message message) {
            receivedMessages.computeIfAbsent(from, k -> new ArrayList<>()).add(message);
        }
    }

    private static class TestServerObserver implements ServerObserver {

        public final List<Message> sentMessages = new ArrayList<>();
        public final List<Message> receivedMessages = new ArrayList<>();

        @Override
        public void onSend(final Message message) {
            sentMessages.add(message);
        }

        @Override
        public void onReceive(final Message message) {
            receivedMessages.add(message);
        }
    }
}
