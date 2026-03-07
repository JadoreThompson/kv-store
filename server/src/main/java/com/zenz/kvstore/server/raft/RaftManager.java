package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.SocketServer;
import com.zenz.kvstore.server.logHandlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.messages.LeaderElected;
import com.zenz.kvstore.server.raft.messages.RequestVote;
import com.zenz.kvstore.server.raft.messages.RequestVoteResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RaftManager {
    private final KVStore store;
    private final ArrayList<RaftNode> brokerConfigs;
    private RaftNode controllerNode;
    private final RaftNode node;
    private ExecutorService executor;
    private final ArrayList<RaftBrokerClient> brokerClients = new ArrayList<>();
    private RaftControllerClient controllerClient;
    private RaftBrokerServerHandler brokerServerHandler;
    private RaftControllerServerHandler controllerServerHandler;
    private SocketServer nodeServer;
    private boolean isRunning;
    private volatile NodeState state;
    private ElectionMeta electionMeta;
    private long votedTerm;
    private long lastTerm;
    private CompletableFuture<Boolean> joinFut;

    private final String DEBUG_PREFIX;

    public RaftManager(long id, ArrayList<RaftNode> nodes, KVStore store) {
        ArrayList<RaftNode> nodesCopy = (ArrayList<RaftNode>) nodes.clone();

        RaftNode node = null;
        for (RaftNode n : nodesCopy) {
            if (n.id() == id) {
                node = n;
                break;
            }
        }

        if (node == null) {
            throw new IllegalArgumentException("Failed to find node with id " + id);
        }

        this.node = node;
        nodesCopy.removeIf((n) -> n.id() == id);
        brokerConfigs = nodesCopy;
        this.store = store;
        state = node.state();
        lastTerm = ((RaftLogHandler) store.getLogHandler()).getTerm();
        DEBUG_PREFIX = String.format("[nodeId=%s RaftManager]", id);
    }

    public void start() {
        final String DBG_PREFIX = String.format("[nodeId=%s start]", node.id());

        if (isRunning) {
            return;
        }

        isRunning = true;

        executor = Executors.newFixedThreadPool(brokerConfigs.size() + 1);
        nodeServer = new SocketServer(node.nodeAddress().getHostName(), node.nodeAddress().getPort());

        if (state == NodeState.BROKER) {
            startControllerClient();

            brokerServerHandler = new RaftBrokerServerHandler(nodeServer, this);
            nodeServer.setSocketHandler(brokerServerHandler);
            executor.submit(() -> wrapper(nodeServer::start));

            for (RaftNode broker : brokerConfigs) {
                startBrokerClient(broker);
            }
        } else if (state == NodeState.CONTROLLER) {
            controllerServerHandler = new RaftControllerServerHandler(
                    nodeServer,
                    (RaftLogHandler) store.getLogHandler(),
                    store.getSnapshotter(),
                    this
            );
            nodeServer.setSocketHandler(controllerServerHandler);
            executor.submit(() -> wrapper(nodeServer::start));
        }
    }

    private void wrapper(CheckedRunnable r) {
        try {
            r.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startBrokerClient(RaftNode broker) {
        RaftBrokerClient brokerClient = new RaftBrokerClient(
                broker.nodeAddress().getHostName(),
                broker.nodeAddress().getPort(),
                this
        );
        brokerClients.add(brokerClient);
        executor.submit(() -> wrapper(brokerClient::start));
    }

    private void startControllerClient() {
        RaftNode node = null;
        for (RaftNode n : brokerConfigs) {
            if (n.state() == NodeState.CONTROLLER) {
                node = n;
                break;
            }
        }

        if (node == null) {
            throw new RuntimeException("Failed to find controller node in config");
        }

        controllerNode = node;
        controllerClient = new RaftControllerClient(
                controllerNode.nodeAddress().getHostName(),
                controllerNode.nodeAddress().getPort(),
                store,
                this
        );
        executor.submit(() -> wrapper(controllerClient::start));
        brokerConfigs.remove(controllerNode);

        joinFut = new CompletableFuture<>();
    }

    public void stop() throws IOException, InterruptedException {
        if (!isRunning) {
            return;
        }

        isRunning = false;

        // Stop broker clients
        for (int i = 0; i < brokerClients.size(); i++) {
            RaftBrokerClient brokerClient = brokerClients.get(i);
            brokerClient.stop();
        }

        // Stop controller client
        if (controllerClient != null && controllerClient.isRunning()) {
            controllerClient.stop();
        }

        if (nodeServer != null && nodeServer.isRunning()) {
            nodeServer.stop();
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        if (joinFut != null) joinFut.complete(true);
    }

    public void join() {
        try {
            joinFut.get();
        } catch (Exception e) {
        }
    }

    /**
     * Handles the full election process when a controller crashes.
     */
    public void initiateElectionAsControllerClient() throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[initiateElectionAsControllerClient] ";

        int count = 1;
        for (var client : brokerClients) {
            boolean running = client.isRunning();
            if (running) count++;
        }

        int majority = count / 2 + 1;
        long curTerm = controllerClient.getLogHandler().getTerm();

        electionMeta = new ElectionMeta(
                0,
                lastTerm + 1,
                controllerClient.getLogHandler().getLogId(),
                curTerm,
                majority,
                System.currentTimeMillis() + 1000
        );

        state = NodeState.CANDIDATE;
        handleVoteResponse(new RequestVoteResponse(true, electionMeta.getTerm()));
    }

    public boolean shouldGrantVote(RequestVote message) {
        final String DBG_PREFIX = String.format("[nodeId=%s shouldGrantVote]", node.id());

        if (message.term() <= votedTerm) {
            return false;
        }

        long currentTerm = ((RaftLogHandler) store.getLogHandler()).getTerm();
        long currentLogId = store.getLogHandler().getLogId();


        if ((message.term() > currentTerm) || (message.logId() > currentLogId)) {
            votedTerm = message.term();
            return true;
        }
        return false;
    }

    /**
     * Increments the current vote count and asserts the new vote count
     * is greater than or equal to the majority of nodes. If true,
     * the controller server is instantiated and broker server brought down.
     * The state is then updated to CONTROLLER.
     *
     * @param response - The response to the vote request.
     * @throws IOException
     */
    public void handleVoteResponse(RequestVoteResponse response) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[handleVoteResponse] ";

        if (electionMeta == null) {
            return;
        }

        if (response.term() != electionMeta.getTerm()) {
            return;
        }

        boolean voteGranted = response.voteGranted();

        if (voteGranted) {
            electionMeta.voteCount++;
        }

        if (electionMeta.voteCount >= electionMeta.majority) {
            controllerServerHandler = new RaftControllerServerHandler(
                    nodeServer,
                    (RaftLogHandler) store.getLogHandler(),
                    store.getSnapshotter(),
                    this
            );
            controllerServerHandler.init();
            nodeServer.setSocketHandler(controllerServerHandler);
            brokerServerHandler = null;

            // Notifying brokers of new leader and closing connections
            state = NodeState.CONTROLLER;
        }
    }

    /**
     * Updates state to follower and re-establishes controller client.
     * <p>
     * If the node with leader id `message.leaderId` is present within the config.
     * A new client is created, connecting to the nodeAddress as the controller.
     * The old controller client is dismantled.
     *
     * @param message - The message
     * @throws IOException
     */
    public void handleLeaderElected(LeaderElected message) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[handleLeaderElected] ";

        if (state == NodeState.CONTROLLER) {
            throw new RuntimeException("Leader elected whilst node is CONTROLLER");
        }

        state = NodeState.BROKER;
        electionMeta = null;
        RaftNode brokerNode = null;
        for (RaftNode broker : brokerConfigs) {
            if (broker.id() == message.leaderId()) {
//                remoteAddr = broker.nodeAddress();
                brokerNode = broker;
                break;
            }
        }

        if (brokerNode == null) {
            throw new RuntimeException("Failed to find node for broker with node id " + message.leaderId());
        }

        // Update term
        RaftLogHandler logHandler = (RaftLogHandler) store.getLogHandler();
        logHandler.setTerm(message.term());

        // Re-establishing controller
        controllerClient.stop();
        controllerClient = new RaftControllerClient(
                brokerNode.nodeAddress().getHostName(),
                brokerNode.nodeAddress().getPort(),
                store,
                this
        );
        executor.submit(() -> wrapper(controllerClient::start));

        brokerConfigs.remove(brokerNode);
    }

    public boolean isRunning() {
        return nodeServer.isRunning();
    }

    public RaftControllerClient getControllerClient() {
        return controllerClient;
    }

    public ArrayList<RaftBrokerClient> getBrokerClients() {
        return brokerClients;
    }

    public RaftNode getConfig() {
        return node;
    }

    public NodeState getState() {
        return state;
    }

    public ElectionMeta getElectionMeta() {
        return electionMeta;
    }

    public KVStore getKVStore() {
        return store;
    }

    public void setLastTerm(long term) {
        if (term > lastTerm) {
            lastTerm = term;
        }
    }

    public SocketServer getNodeServer() {
        return nodeServer;
    }

    public RaftControllerServerHandler getControllerServerHandler() {
        return controllerServerHandler;
    }

    public RaftBrokerServerHandler getBrokerServerHandler() {
        return brokerServerHandler;
    }

    public void convertControllerToFollower() {
    }

    public RaftNode getControllerNode() {
        return controllerNode;
    }

    interface CheckedRunnable {
        void run() throws Exception;
    }

    public class ElectionMeta {
        private int voteCount;
        private final long term;
        private final long prevLogId;
        private final long prevTerm;
        private int majority;
        private long electionDeadline;

        public ElectionMeta(
                int voteCount,
                long term,
                long prevLogId,
                long prevTerm,
                int majority,
                long electionDeadline
        ) {
            this.voteCount = voteCount;
            this.term = term;
            this.prevLogId = prevLogId;
            this.prevTerm = prevTerm;
            this.majority = majority;
            this.electionDeadline = electionDeadline;
        }

        public int getVoteCount() {
            return voteCount;
        }

        private void setVoteCount(int voteCount) {
            this.voteCount = voteCount;
        }

        public long getTerm() {
            return term;
        }

        public long getPrevLogId() {
            return prevLogId;
        }

        public long getPrevTerm() {
            return prevTerm;
        }

        public int getMajority() {
            return majority;
        }

        private void setMajority(int majority) {
            this.majority = majority;
        }

        public KVStore getStore() {
            return store;
        }

        public long getElectionDeadline() {
            return electionDeadline;
        }

        public boolean updateElectionDeadlineIfExpired(long electionDeadline) {
            if (System.currentTimeMillis() > this.electionDeadline) {
                this.electionDeadline = electionDeadline;
                return true;
            }

            return false;
        }

        @Override
        public String toString() {
            return "ElectionMeta{" +
                    "voteCount=" + voteCount +
                    ", term=" + term +
                    ", prevLogId=" + prevLogId +
                    ", prevTerm=" + prevTerm +
                    ", majority=" + majority +
                    ", electionDeadline=" + electionDeadline +
                    '}';
        }
    }
}
