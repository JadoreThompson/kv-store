package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.exception.ResourceNotFoundException;
import com.zenz.kvstore.server.logging.NoOpLogHandler;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.RequestVoteResponse;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Manager implements Closeable {

    @Getter
    @Setter
    private KVStore kvstore;

    @Getter
    private final NodeConfig nodeConfig;

    @Getter
    private final List<NodeConfig> peerConfigs = new ArrayList<>();

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Getter
    private Server server;

    @Getter
    private final List<Client> clients = new ArrayList<>();
    private final Map<Client, NodeConfig> clientNodeConfigMap = new HashMap<>();
    private Client leaderClient;
    @Getter
    private final StateObject stateObject = new StateObject();
    @Getter
    private boolean isOpen;

    @Getter
    @Setter
    private ClientObserver clientObserver = new RaftClientObserver();

    @Getter
    @Setter
    private ServerObserver serverObserver = new RaftServerObserver();

    // Debug
    private final Logger log;

    public Manager(final KVStore kvStore, final NodeConfig nodeConfig, final List<NodeConfig> peerConfigs) {
        this.kvstore = kvStore;
        stateObject.setLogHandler((RaftLogHandler) kvstore.getLogHandler());
        kvstore.setLogHandler(new NoOpLogHandler<>());
        this.nodeConfig = nodeConfig;
        this.peerConfigs.addAll(peerConfigs);
        log = LoggerFactory.getLogger(String.format("[%s][Manager]", nodeConfig.id()));
    }

    public void open() throws InterruptedException {
        server = new Server(nodeConfig.address());
        server.setManager(this);
        server.setStateObject(stateObject);
        server.setObserver(serverObserver);
        executorService.submit(runnableWrapper(server::open));

        while (!server.isRunning()) {
            log.info("Waiting for server to start");
            Thread.sleep(1000);
        }

        log.info("Server started");
        isOpen = true;

        for (NodeConfig config : peerConfigs) {
            final Client client = new Client(config.address());
            client.setManager(this);
            client.setStateObject(stateObject);
            client.setObserver(clientObserver);
            clientNodeConfigMap.put(client, config);
            executorService.submit(runnableWrapper(client::open));
            clients.add(client);
        }
        log.info("Started " + peerConfigs.size() + " peer clients");

        final Set<Client> connectedClients = new HashSet<>();
        while (connectedClients.size() < peerConfigs.size()) {
            for (int i = 0; i < peerConfigs.size(); i++) {
                final Client client = clients.get(i);
                if (client.isConnected() && !connectedClients.contains(client)) {
                    log.info("Connected to client {}", peerConfigs.get(i));
                    connectedClients.add(client);
                }
            }

            log.info("Connected to " + connectedClients.size() + "/" + peerConfigs.size() + " clients");
            Thread.sleep(1000);
        }

        watch();
    }

    private void watch() throws InterruptedException {
        final Random random = new Random();
        boolean electionStarted = false;

        while (isOpen) {
            final int sleepTime = random.nextInt(1000, 1500);
            Thread.sleep(sleepTime);
            if (stateObject.state != State.FOLLOWER) {
                continue;
            }

            if (leaderClient == null) {
                String message = "Leader client is null.";
                if (!electionStarted) {
                    message += " Starting election";
                    electionStarted = true;
                    startElection();
                }
                log.info(message);
            } else if (System.currentTimeMillis() - server.getLastAppendEntryTs() > sleepTime) {
                log.info("Leader client message timed out");
                startElection();
                leaderClient = null;
                electionStarted = false;
            }
        }
    }

    private Runnable runnableWrapper(final CheckedRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };
    }

    /**
     * Replicates the log entry derived from this command across follower nodes
     *
     * @return A future which completes when a majority of nodes have committed the entry to their logs
     */
    public CompletableFuture<Boolean> handleCommand(final Command command) throws IOException {
        final long prevLogId = stateObject.getLogHandler().getLogId();
        final long prevLogTerm = stateObject.getLogHandler().getTerm();
        final StateObject.ReplicateTask replicateTask = new StateObject.ReplicateTask(
                stateObject.getLogHandler().log(command),
                prevLogId,
                prevLogTerm,
                new CompletableFuture<>(),
                1 + (1 + peerConfigs.size() / 2)
        );
        stateObject.setReplicateTask(replicateTask);
        return replicateTask.getFuture();
    }

    public StateObject.Election startElection() {
        final int majority = 1 + (1 + peerConfigs.size() / 2);
        final long term = stateObject.setCurrentTerm(stateObject.getCurrentTerm() + 1);
        stateObject.election = new StateObject.Election(term, majority, System.currentTimeMillis() + 15000);
        stateObject.state = State.CANDIDATE;
        stateObject.votedTerm = term;
        handleRequestVoteResponse(new RequestVoteResponse(term, true));
        return stateObject.election;
    }

    public void handleRequestVoteResponse(final RequestVoteResponse response) {
        final StateObject.Election election = stateObject.getElection();
        if (election == null || election.isDone() || election.term < stateObject.getCurrentTerm()) {
            return;
        }

        synchronized (election) {
            if (response.voteGranted()) {
                ++election.voteCount;
                if (election.isDone()) {
                    stateObject.setState(State.LEADER);
                    stateObject.setLeaderId(nodeConfig.id());
                }
            }
        }
    }

    void handleClientClose(final Client client) {
        log.info("Closing client {}", client);
        synchronized (clients) {
            clients.remove(client);
        }
        synchronized (clientNodeConfigMap) {
            final NodeConfig config = clientNodeConfigMap.remove(client);
            synchronized (peerConfigs) {
                peerConfigs.remove(config);
            }
        }

        final StateObject.Election election = stateObject.election;
        if (election != null && !election.isDone() && !election.isExpired()) {
            synchronized (election) {
                ++election.voteCount;
                if (election.isDone()) {
                    stateObject.setState(State.LEADER);
                }
            }
        }

        final StateObject.ReplicateTask replicateTask = stateObject.getReplicateTask();
        if (replicateTask != null && !replicateTask.isDone()) {
            replicateTask.incrementCount();
        }
    }

    public void close() throws IOException {
        if (!isOpen) {
            return;
        }

        isOpen = false;
        final List<Client> clients = new ArrayList<>(this.clients);
        for (Client client : clients) {
            client.close();
        }

        server.close();
    }

    public void setLeader(final String leaderId) {
        stateObject.setState(State.FOLLOWER);
        stateObject.setReplicateTask(null);
        stateObject.setLeaderId(leaderId);
        for (Map.Entry<Client, NodeConfig> entry : clientNodeConfigMap.entrySet()) {
            if (entry.getValue().id().equals(leaderId)) {
                leaderClient = entry.getKey();
                return;
            }
        }
        throw new ResourceNotFoundException("Failed to find client for leader '" + leaderId + "'");
    }

    private interface CheckedRunnable {

        void run() throws IOException;
    }
}
