package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.utils.Utils;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
import com.zenz.kvstore.server.raft.server.SocketServer;
import com.zenz.kvstore.server.raft.server.handler.RaftBrokerServerHandler;
import com.zenz.kvstore.server.raft.server.handler.RaftControllerServerHandler;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RaftManager {
    private final KVStore kvStore;

    private NodeConfig nodeConfig;
    //    private List<NodeConfig> nodeConfigs = new ArrayList<>();
    private List<NodeConfig> nodeConfigs = new ArrayList<>();
    private NodeConfig controllerConfig;
    private RaftServer server;
    private RaftBrokerServerHandler brokerServerHandler;
    private RaftControllerServerHandler controllerServerHandler;

    private ExecutorService executorService;
    private ArrayList<RaftBrokerClient> brokerClients = new ArrayList<>();
    private RaftControllerClient controllerClient;

    private volatile NodeRole role = NodeRole.BROKER;
    private ElectionMeta electionMeta;
    private long votedTerm;
    private long lastSeenTerm;

    private boolean isRunning;
    private CompletableFuture<Boolean> joinFut;


    private ArrayList<RaftNodeConfig> brokerConfigs;

    private RaftNodeConfig node;

    private RaftNodeConfig controllerNode;

    private SocketServer nodeServer;

    public RaftManager(long id, ArrayList<RaftNodeConfig> nodes, KVStore store) {
        ArrayList<RaftNodeConfig> nodesCopy = (ArrayList<RaftNodeConfig>) nodes.clone();

        RaftNodeConfig node = null;
        for (RaftNodeConfig n : nodesCopy) {
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
        this.kvStore = store;
        role = node.state();
        lastSeenTerm = ((RaftLogHandler) store.getLogHandler()).getTerm();
    }

    public RaftManager(NodeConfig nodeConfig, KVStore kvStore) {
        this.nodeConfig = nodeConfig;
        this.kvStore = kvStore;
    }

    public void start() {
        final String DBG_PREFIX = String.format("[nodeId=%s start]", node.id());

        if (isRunning) {
            return;
        }

        isRunning = true;

        executorService = Executors.newFixedThreadPool(brokerConfigs.size() + 1);
        nodeServer = new SocketServer(node.nodeAddress().getHostName(), node.nodeAddress().getPort());

        if (role == NodeRole.BROKER) {
            startControllerClient();

            brokerServerHandler = new RaftBrokerServerHandler(nodeServer, this);
            nodeServer.setSocketHandler(brokerServerHandler);
            executorService.submit(() -> Utils.checkedRunnableWrapper(nodeServer::start));

            for (RaftNodeConfig broker : brokerConfigs) {
                startBrokerClient(broker);
            }
        } else if (role == NodeRole.CONTROLLER) {
            controllerServerHandler = new RaftControllerServerHandler(
                    nodeServer,
                    (RaftLogHandler) kvStore.getLogHandler(),
                    kvStore.getSnapshotter(),
                    this
            );
            nodeServer.setSocketHandler(controllerServerHandler);
            executorService.submit(() -> Utils.checkedRunnableWrapper(nodeServer::start));
        }
    }

    public void start(List<NodeConfig> nodeConfigs) throws IOException {
        if (isRunning) return;

        isRunning = true;
        executorService = Executors.newCachedThreadPool();
        this.nodeConfigs.addAll(nodeConfigs);

        server = new RaftServer(nodeConfig.serverAddress(), this);
        executorService.submit(() -> Utils.checkedRunnableWrapper(server::start));

        startBrokerClients();

        NodeConfig controllerConfig = findController(nodeConfigs);
        if (controllerConfig == null) {
            System.err.println("Failed to find controller config");
            initiateElection();
        } else {
            // Disconnecting from controller
            for (RaftBrokerClient brokerClient : brokerClients) {
                if (brokerClient.getRemoteAddress() == controllerConfig.serverAddress()) {
                    brokerClient.stop();
                }
            }
            this.controllerConfig = controllerConfig;
            // Connecting to controller
            controllerClient = new RaftControllerClient(
                    controllerConfig.serverAddress().getHostName(),
                    controllerConfig.serverAddress().getPort(),
                    kvStore,
                    this
            );
            executorService.submit(() -> Utils.checkedRunnableWrapper(controllerClient::start));
        }
    }

    /**
     * Iterates through the known peers sending a heartbeat request
     *
     * @param nodeConfigs
     */
    private NodeConfig findController(List<NodeConfig> nodeConfigs) throws IOException {
        for (NodeConfig n : nodeConfigs) {
            Message response = sendHeartbeatRequest(n);

            if (response.type().equals(MessageType.HEARTBEAT_RESPONSE)) {
                return n;
            }

            if (response.type().equals(MessageType.REDIRECT)) {
                NodeConfig redirectNode = ((RedirectMessage) response).node();
                response = sendHeartbeatRequest(redirectNode);
                if (response.type().equals(MessageType.HEARTBEAT_RESPONSE)) {
                    return redirectNode;
                }
            }
        }

        return null;
    }

    private Message sendHeartbeatRequest(NodeConfig nodeConfig) throws IOException {
        try (Socket socket = new Socket(nodeConfig.serverAddress().getHostName(), nodeConfig.serverAddress().getPort())) {
            BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
            BufferedInputStream in = new BufferedInputStream(socket.getInputStream());

            out.write(ByteBuffer.wrap(new HeartbeatRequest().serialize()).array());
            ByteBuffer responseBuffer = ByteBuffer.wrap(in.readAllBytes());
            return Message.deserialize(responseBuffer);
        }
    }

    /**
     * DANGER
     */
    public void startBrokerClients() {
        for (NodeConfig n : nodeConfigs) {
            startBrokerClient(n);
        }
    }

    private void startBrokerClient(RaftNodeConfig broker) {
        RaftBrokerClient brokerClient = new RaftBrokerClient(
                broker.nodeAddress().getHostName(),
                broker.nodeAddress().getPort(),
                this
        );
        brokerClients.add(brokerClient);
        executorService.submit(() -> Utils.checkedRunnableWrapper(brokerClient::start));
    }

    /**
     * Starts a broker client in a background thread
     *
     * @param node
     */
    private void startBrokerClient(NodeConfig node) {
        RaftBrokerClient brokerClient = new RaftBrokerClient(
                node.serverAddress().getHostName(),
                node.serverAddress().getPort(),
                this
        );
        brokerClients.add(brokerClient);
        executorService.submit(() -> Utils.checkedRunnableWrapper(brokerClient::start));
    }

    private void startControllerClient() {
        RaftNodeConfig node = null;
        for (RaftNodeConfig n : brokerConfigs) {
            if (n.state() == NodeRole.CONTROLLER) {
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
                kvStore,
                this
        );
        executorService.submit(() -> Utils.checkedRunnableWrapper(controllerClient::start));
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

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

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
    public void initiateElection() throws IOException {
        int count = 1;
        for (var client : brokerClients) {
            boolean running = client.isRunning();
            if (running) count++;
        }

        int majority = count / 2 + 1;
        long lastTerm = this.lastSeenTerm;

        electionMeta = new ElectionMeta(
                0,
                ++this.lastSeenTerm,
                controllerClient.getLogHandler().getLogId(),
                lastTerm,
                majority,
                System.currentTimeMillis() + 1000
        );

        role = NodeRole.CANDIDATE;
        handleVoteResponse(new RequestVoteResponse(true, electionMeta.getTerm()));
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
        if (electionMeta == null) {
            return;
        }

        if (response.term() != electionMeta.getTerm()) {
            return;
        }

        boolean voteGranted = response.voteGranted();

        if (voteGranted) {
            electionMeta.voteCount++;

            if (electionMeta.voteCount >= electionMeta.majority && controllerServerHandler == null) {
                controllerServerHandler = new RaftControllerServerHandler(
                        nodeServer,
                        (RaftLogHandler) kvStore.getLogHandler(),
                        kvStore.getSnapshotter(),
                        this
                );
                controllerServerHandler.init();
                nodeServer.setSocketHandler(controllerServerHandler);
                brokerServerHandler = null;

                // Notifying brokers of new leader and closing connections
                role = NodeRole.CONTROLLER;
            }
        }
    }

    /**
     * Updates state to follower and re-establishes controller client.
     * <p>
     * If the node with leader id `message.name` is present within the config.
     * A new client is created, connecting to the nodeAddress as the controller.
     * The old controller client is dismantled.
     *
     * @param message - The message
     * @throws IOException
     */
    public void handleLeaderElected(LeaderElected message) throws IOException {
        if (role == NodeRole.CONTROLLER) {
            throw new RuntimeException("Leader elected whilst node is CONTROLLER");
        }

        role = NodeRole.BROKER;
        electionMeta = null;
        RaftNodeConfig brokerNode = null;
//        for (RaftNodeConfig broker : brokerConfigs) {
//            if (broker.id() == message.name()) {
//                brokerNode = broker;
//                break;
//            }
//        }

        if (brokerNode == null) {
            throw new RuntimeException("Failed to find node for broker with node id " + message.name());
        }

        // Update term
        RaftLogHandler logHandler = (RaftLogHandler) kvStore.getLogHandler();
        logHandler.setTerm(message.term());

        // Re-establishing controller
        controllerClient.stop();
        controllerClient = new RaftControllerClient(
                brokerNode.nodeAddress().getHostName(),
                brokerNode.nodeAddress().getPort(),
                kvStore,
                this
        );
        executorService.submit(() -> Utils.checkedRunnableWrapper(controllerClient::start));

        brokerConfigs.remove(brokerNode);
    }

    public void handleLeaderElectedNew(LeaderElected message) throws IOException {
        if (role == NodeRole.CONTROLLER) {
            throw new RuntimeException("Leader elected whilst node is CONTROLLER");
        }

        role = NodeRole.BROKER;
        electionMeta = null;
        NodeConfig brokerConfig = null;

        for (NodeConfig c : nodeConfigs) {
            if (c.name().equals(message.name())) {
                brokerConfig = c;
                break;
            }
        }

        if (brokerConfig == null) {
            throw new RuntimeException("Failed to find node for broker with node name " + message.name());
        }

        for (RaftBrokerClient brokerClient : brokerClients) {
            if (brokerClient.getRemoteAddress().equals(brokerConfig.serverAddress())) {
                brokerClient.stop();
                break;
            }
        }

        // Update term
        RaftLogHandler logHandler = (RaftLogHandler) kvStore.getLogHandler();
        logHandler.setTerm(message.term());

        // Re-establishing controller
        controllerClient.stop();
        controllerClient = new RaftControllerClient(
                brokerConfig.serverAddress().getHostName(),
                brokerConfig.serverAddress().getPort(),
                kvStore,
                this
        );
        executorService.submit(() -> Utils.checkedRunnableWrapper(controllerClient::start));
    }

    /**
     * Starts a broker client connecting to the node if we haven't already
     * connected to it.
     *
     * @param message
     * @throws IOException
     */
    public void handleRegisterMessage(RegisterMessage message) throws IOException {
        NodeConfig nConfig = new NodeConfig(message.name(), message.serverAddress(), null);
        if (role.equals(NodeRole.BROKER)) {
            if (controllerConfig.equals(nConfig)) {
                return;
            }

            for (RaftBrokerClient brokerClient : brokerClients) {
                if (brokerClient.getRemoteAddress().equals(message.serverAddress())) {
                    return;
                }
            }

            startBrokerClient(nConfig);
        }

        if (nodeConfigs.stream().noneMatch(n -> n.equals(nConfig))) {
            nodeConfigs.add(nConfig);
        }
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

    public RaftNodeConfig getConfig() {
        return node;
    }

    public NodeConfig getNodeConfig() {
        return nodeConfig;
    }

    public NodeRole getRole() {
        return role;
    }

    public void setRole(NodeRole role) {
        this.role = role;
    }

    public ElectionMeta getElectionMeta() {
        return electionMeta;
    }

    public KVStore getKVStore() {
        return kvStore;
    }

    /**
     * Sets the last seen term
     *
     * @param term
     */
    public void setLastSeenTerm(long term) {
        if (term > lastSeenTerm) {
            lastSeenTerm = term;
        }
    }

    public SocketServer getNodeServer() {
        return nodeServer;
    }

    public RaftServer getServer() {
        return server;
    }

    public RaftControllerServerHandler getControllerServerHandler() {
        return controllerServerHandler;
    }

    public RaftBrokerServerHandler getBrokerServerHandler() {
        return brokerServerHandler;
    }

    public void convertControllerToFollower() {
    }

    @Deprecated
    public RaftNodeConfig getControllerNode() {
        return controllerNode;
    }

    public long getVotedTerm() {
        return votedTerm;
    }

    public long getLastSeenTerm() {
        return lastSeenTerm;
    }

    public void setVotedTerm(long term) {
        votedTerm = term;
    }

    public NodeConfig getControllerConfig() {
        return controllerConfig;
    }

    public class ElectionMeta {
        private int voteCount;
        private final long term;
        private final long prevLogId;
        private final long prevTerm;
        private final int majority;
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

        public KVStore getStore() {
            return kvStore;
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
