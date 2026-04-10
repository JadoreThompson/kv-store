package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.utils.Utils;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class Manager {

    private final KVStore kvstore;
    private final NodeConfig nodeConfig;
    private final List<NodeConfig> nodeConfigs = new ArrayList<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    // Servers and clients
    private final ArrayList<BrokerClient> brokerClients = new ArrayList<>();
    volatile private NodeRole role = NodeRole.BROKER;
    volatile private ControllerClient controllerClient;
    private NodeConfig controllerConfig;
    private Server server;

    // Election
    private Election election;
    volatile private long lastVotedTerm;
    volatile private long currentTerm;
    private final Object currentTermLock = new Object();

    // Heartbeat
    private long lastHeartbeatTs;
    volatile private boolean isBootstrapping = true;

    // Debug
    volatile private boolean isLoggingEnabled = true;
    private final String DEBUG_PREFIX;

    /**
     * @param kvstore     The KV store to be used by this node
     * @param nodeConfigs List of nodes within the network
     */
    public Manager(final KVStore kvstore, final NodeConfig nodeConfig, final List<NodeConfig> nodeConfigs) {
        this.kvstore = kvstore;
        this.nodeConfig = nodeConfig;
        this.nodeConfigs.addAll(nodeConfigs);
        this.currentTerm = ((RaftLogHandler) kvstore.getLogHandler()).getTerm();
        this.DEBUG_PREFIX = String.format("[%s][Manager]", nodeConfig.name());
    }

    /**
     * Bootstraps the node to the container. Launches the server for the node. Establishes connections to peers within the
     * cluster and looks to find the controller before attempting to become it
     */
    public void start() throws IOException, InterruptedException {
        final String debugPrefix = DEBUG_PREFIX + "[start] ";

        this.server = new Server(this.nodeConfig.serverAddress(), this);
        this.executorService.submit(() -> Utils.checkedRunnableWrapper(this.server::start));
        for (NodeConfig nodeConfig : this.nodeConfigs) {
            startBrokerClient(nodeConfig);
        }

        final Set<BrokerClient> readyBrokerClients = new HashSet<>();
        while (readyBrokerClients.size() < this.nodeConfigs.size()) {
            for (BrokerClient brokerClient : this.brokerClients) {
                if (brokerClient.getStatus() == ClientStatus.CONNECTED) {
                    readyBrokerClients.add(brokerClient);
                }
            }

            log(
                    debugPrefix +
                            readyBrokerClients.size() +
                            " out of " + this.nodeConfigs.size() + " brokers are ready");

            Thread.sleep(100);
        }

        this.controllerConfig = findController(this.nodeConfigs);
        log(debugPrefix + "Controller config: " + this.controllerConfig);
        if (this.controllerConfig != null) {
            this.controllerClient = new ControllerClient(this.controllerConfig.serverAddress(), this);
            this.executorService.submit(() -> Utils.checkedRunnableWrapper(this.controllerClient::start));
        }

        watch();
    }

    public void stop() throws IOException {
        if (this.server != null && this.server.isRunning()) {
            this.server.stop();
        }
    }

    /**
     * Checks whether to start an election. If the role is BROKER (follower) and the controller (leader) client is null
     * or the controller client isn't connected to the remote then an election is initiated.
     *
     * @throws InterruptedException
     */
    private void watch() throws InterruptedException, IOException {
        final String debugPrefix = this.DEBUG_PREFIX + "[watch] ";

        final Random random = new Random();
        final Supplier<Integer> generateHeartbeatMs = () -> random.nextInt(500, 1500);

        if (this.controllerClient != null) {
            this.controllerClient.sendHeartbeat();
        }

        while (this.server.isRunning()) {
            final int heartbeatTimeoutMs = generateHeartbeatMs.get();
            Thread.sleep(heartbeatTimeoutMs);
            if (this.role != NodeRole.BROKER) {
                continue;
            }

            // Bootstrapping
            if (this.controllerClient == null) {
                if (this.isBootstrapping) {
                    startElection();
                }
                continue;
            }

            if (System.currentTimeMillis() - this.lastHeartbeatTs > heartbeatTimeoutMs) {
                this.controllerClient.stop();
                startElection();
                continue;
            }

            this.controllerClient.sendHeartbeat();
        }
    }

    private void startBrokerClient(NodeConfig nodeConfig) {
        BrokerClient brokerClient = new BrokerClient(nodeConfig.serverAddress(), this);
        synchronized (this.brokerClients) {
            this.brokerClients.add(brokerClient);
        }
        this.executorService.submit(() -> Utils.checkedRunnableWrapper(brokerClient::start));
    }

    /**
     * Iterates through a list of node configurations sending heartbeat requests to each. If the node responds
     * with a heartbeat response then that node must be the controller.
     *
     * @param nodeConfigs Node configurations to iterate
     * @return The NodeConfig object of the controller.
     * @throws IOException
     * @throws InterruptedException
     */
    public NodeConfig findController(List<NodeConfig> nodeConfigs) throws IOException, InterruptedException {
        final String debugPrefix = this.DEBUG_PREFIX + "[findController] ";
        for (NodeConfig n : nodeConfigs) {
            Message response = sendHeartbeatRequest(n);
            if (response == null) {
                continue;
            }

            if (response.type().equals(MessageType.HEARTBEAT_RESPONSE)) {
                return n;
            }

            if (response.type().equals(MessageType.REDIRECT)) {
                NodeConfig redirectNode = ((RedirectMessage) response).node();
                response = sendHeartbeatRequest(redirectNode);
                if (response != null && response.type().equals(MessageType.HEARTBEAT_RESPONSE)) {
                    return redirectNode;
                }
            }
        }

        return null;
    }

    private Message sendHeartbeatRequest(NodeConfig nodeConfig) throws IOException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            try (Socket socket =
                         new Socket(nodeConfig.serverAddress().getHostName(), nodeConfig.serverAddress().getPort())) {
                socket.setSoTimeout(1000);
                BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());

                out.write(new HeartbeatRequest().serialize());
                out.flush();
                return Message.deserialize(ByteBuffer.wrap(in.readNBytes(4)));
            } catch (ConnectException | SocketTimeoutException e) {
            }

            Thread.sleep((i + 1) * 100);
        }

        return null;
    }

    /**
     * Sets role to CANDIDATE, creates election object and votes for itself
     */
    public void startElection() {
        final String debugPrefix = this.DEBUG_PREFIX + "[startElection] ";
        int brokerCount = 1;
        synchronized (this.brokerClients) {
            brokerCount += (int) this.brokerClients
                    .stream()
                    .filter((brokerClient -> brokerClient.getStatus() == ClientStatus.CONNECTED))
                    .count();
        }

        log(debugPrefix + "Cluster size: " + brokerCount + ", broker count: " + (brokerCount - 1));
        final int majority = brokerCount / 2 + 1;

        synchronized (this.currentTermLock) {
            final long prevTerm = this.currentTerm;
            this.currentTerm++;
            this.election = new Election(this.currentTerm, kvstore.getLogHandler().getLogId(), prevTerm, majority);
            log(debugPrefix + "Started election: " + this.election);
        }

        this.role = NodeRole.CANDIDATE;
        this.lastVotedTerm = this.currentTerm;
        handleVoteResponse(new RequestVoteResponse(true, this.election.term));
    }

    public void handleVoteResponse(RequestVoteResponse response) {
        final String debugPrefix = this.DEBUG_PREFIX + "[handleVoteResponse] ";
        log(debugPrefix + "Handling vote response: " + response);
        if (role != NodeRole.CANDIDATE) {
            return;
        }

        final Election election = this.election;
        if (election == null) {
            throw new RuntimeException("Election is null");
        }

        if (response.term() != election.term) {
            log(debugPrefix + "Term mismatch. response term=" + response.term() + " election term=" + election.term);
            return;
        }

        if (response.voteGranted()) {
            election.voteCount++;
            if (election.voteCount >= election.majority && role == NodeRole.CANDIDATE) {
                this.role = NodeRole.CONTROLLER;
            }
        }
    }

    public void handleLeaderElected(LeaderElected leaderElected) throws IOException {
        final String debugPrefix = this.DEBUG_PREFIX + "[handleLeaderElected] ";
        log(debugPrefix + "Handling leader elected " + leaderElected);
        this.isBootstrapping = false;

        synchronized (this.currentTermLock) {
            if (leaderElected.term() < this.currentTerm) {
                return;
            }
            this.currentTerm = leaderElected.term();
        }

        this.role = NodeRole.BROKER;
        this.election = null;
        final NodeConfig leaderConfig = this.nodeConfigs
                .stream()
                .filter(config -> config.name().equals(leaderElected.name()))
                .findFirst()
                .orElseThrow(
                        () -> new RuntimeException("Failed to find configuration or elected leader " + leaderElected)
                );

        ((RaftLogHandler) this.kvstore.getLogHandler()).setTerm(leaderElected.term());
        if (this.controllerClient != null) {
            this.controllerClient.stop();
        }

        this.controllerClient = new ControllerClient(leaderConfig.serverAddress(), this);
        this.lastHeartbeatTs = System.currentTimeMillis();
        this.controllerConfig = leaderConfig;
        executorService.submit(() -> Utils.checkedRunnableWrapper(this.controllerClient::start));
        this.controllerClient.sendHeartbeat();
    }

    public void handleBrokerClientStop(BrokerClient brokerClient) {
        if (this.role == NodeRole.CANDIDATE && this.election != null) {
            // Incrementing the voteCount to emulate a re-calculation of the majority
            handleVoteResponse(new RequestVoteResponse(true, this.election.term));
        }
        synchronized (this.brokerClients) {
            this.brokerClients.remove(brokerClient);
        }
        this.nodeConfigs.removeIf(
                config -> config != null && config.serverAddress().equals(brokerClient.getRemoteAddress()));
    }

    public void handleRegisterMessage(RegisterMessage registerMessage) {
        final NodeConfig nodeConfig = new NodeConfig(registerMessage.name(), registerMessage.serverAddress(), null);
        this.nodeConfigs.add(nodeConfig);
        startBrokerClient(nodeConfig);
    }

    private void log(final String message) {
        if (this.isLoggingEnabled) {
            System.out.println(message);
        }
    }

    public NodeRole getRole() {
        return role;
    }

    public void setRole(NodeRole role) {
        this.role = role;
    }

    public KVStore getKVStore() {
        return kvstore;
    }

    public List<NodeConfig> getNodeConfigs() {
        return nodeConfigs;
    }

    public long getLastHeartbeatTs() {
        return lastHeartbeatTs;
    }

    public void setLastHeartbeatTs(long lastHeartbeatTs) {
        this.lastHeartbeatTs = lastHeartbeatTs;
    }

    public Election getElection() {
        return election;
    }

    public NodeConfig getNodeConfig() {
        return nodeConfig;
    }

    public long getCurrentTerm() {
        return this.currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        synchronized (this.currentTermLock) {
            if (currentTerm > this.currentTerm) {
                this.currentTerm = currentTerm;
            }
        }
    }

    public long getLastVotedTerm() {
        return lastVotedTerm;
    }

    public void setLastVotedTerm(long lastVotedTerm) {
        this.lastVotedTerm = lastVotedTerm;
    }

    public ControllerClient getControllerClient() {
        return controllerClient;
    }

    public NodeConfig getControllerConfig() {
        return controllerConfig;
    }

    public Server getServer() {
        return server;
    }

    public void setLoggingEnabled(boolean loggingEnabled) {
        this.isLoggingEnabled = loggingEnabled;
    }

    /**
     * Election state object
     */
    public static final class Election {
        private final long term;
        private final long prevLogId;
        private final long prevTerm;
        private final int majority;
        private long voteCount;
        private long deadline;
        private static final int TIMEOUT_MS = 1000;

        private Election(final long term, final long prevLogId, final long prevTerm, final int majority) {
            this.term = term;
            this.prevLogId = prevLogId;
            this.prevTerm = prevTerm;
            this.majority = majority;
            this.deadline = System.currentTimeMillis() + TIMEOUT_MS;
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

        public long getVoteCount() {
            return voteCount;
        }

        public long getDeadline() {
            return deadline;
        }

        /**
         * If deadline is expired a new deadline is created
         *
         * @return Whether the deadline has been surpassed
         */
        public boolean isExpired() {
            return System.currentTimeMillis() > this.deadline;
        }

        @Override
        public String toString() {
            return "Election{" +
                    "term=" + term +
                    ", prevLogId=" + prevLogId +
                    ", prevTerm=" + prevTerm +
                    ", majority=" + majority +
                    ", voteCount=" + voteCount +
                    ", deadline=" + deadline +
                    '}';
        }
    }
}
