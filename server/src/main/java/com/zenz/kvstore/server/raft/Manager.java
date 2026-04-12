package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.exception.ResourceNotFoundException;
import com.zenz.kvstore.server.raft.message.RequestVoteResponse;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class Manager implements Closeable {

    @Getter
    @Setter
    private KVStore kvstore;

    @Getter
    private final NodeConfig nodeConfig;

    @Getter
    private final List<NodeConfig> peerConfigs = new ArrayList<>();

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private Server server;
    private final List<Client> clients = new ArrayList<>();
    private final Map<Client, NodeConfig> clientNodeConfigMap = new HashMap<>();
    private Client leaderClient;
    private final StateObject stateObject = new StateObject();
    @Getter
    private boolean isOpen;

    // Debug
    private final Logger log;
    private final String DEBUG_PREFIX;

    public Manager(final KVStore kvStore, final NodeConfig nodeConfig, final List<NodeConfig> peerConfigs) {
        this.kvstore = kvStore;
        this.nodeConfig = nodeConfig;
        this.peerConfigs.addAll(peerConfigs);
        DEBUG_PREFIX = String.format("[%s][Manager]", nodeConfig.id());
        log = LoggerFactory.getLogger(DEBUG_PREFIX);
    }

    public void open() throws InterruptedException {
        final String debugPrefix = DEBUG_PREFIX + "[open] ";

        server = new Server(nodeConfig.address());
        executorService.submit(runnableWrapper(server::open));

        while (!server.getManager().isOpen()) {
            log.info("Waiting for server to start");
            Thread.sleep(1000);
        }
        log.info("Server started");
        isOpen = true;

        for (NodeConfig config : peerConfigs) {
            final Client client = new Client(config.address());
            client.setManager(this);
            client.setStateObject(stateObject);
            clientNodeConfigMap.put(client, config);
            executorService.submit(runnableWrapper(client::open));
            clients.add(client);
        }

        final Set<Client> connectedClients = new HashSet<>();
        while (connectedClients.size() < peerConfigs.size()) {
            for (int i = 0; i < peerConfigs.size(); i++) {
                final Client client = clients.get(i);
                if (client.isConnected() && !connectedClients.contains(client)) {
                    log.info("Connected to client {}", peerConfigs.get(i));
                    connectedClients.add(client);
                }
            }

            log.info("Connected to " + connectedClients.size() + " clients");
            Thread.sleep(1000);
        }

        watch();
    }

    private void watch() throws InterruptedException {
        final Random random = new Random();

        while (isOpen) {
            final int sleepTime = random.nextInt(500, 1500);
            Thread.sleep(sleepTime);
            if (stateObject.state != State.FOLLOWER) {
                continue;
            }

            if (leaderClient == null) {
                startElection();
            } else if (System.currentTimeMillis() - leaderClient.getLastMessageTs() > sleepTime) {
                startElection();
                leaderClient = null;
            }
        }
    }

    private Runnable runnableWrapper(final CheckedRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public StateObject.Election startElection() {
        final int majority = (1 + peerConfigs.size()) / 2;
        final long term = stateObject.setCurrentTerm(stateObject.getCurrentTerm() + 1);
        stateObject.election = new StateObject.Election(term, majority, System.currentTimeMillis() + 1500);
        stateObject.state = State.CANDIDATE;
        stateObject.votedTerm = term;
        handleRequestVoteResponse(new RequestVoteResponse(term, true));
        return stateObject.election;
    }

    public void handleRequestVoteResponse(final RequestVoteResponse response) {
        final StateObject.Election election = stateObject.election;
        if (election == null || election.isDone() || election.term < stateObject.getCurrentTerm()) {
            return;
        }

        synchronized (election) {
            if (response.voteGranted()) {
                ++election.voteCount;
                if (election.isDone()) {
                    stateObject.state = State.LEADER;
                }
            }
        }
    }

    public Future<Void> replicateCommand(final Command command) {
        return null;
    }

    public void handleClientClose(final Client client) {
        clients.remove(client);
        final NodeConfig config = clientNodeConfigMap.remove(client);
        peerConfigs.remove(config);
        final StateObject.Election election = stateObject.election;
        if (election != null && !election.isDone() && !election.isExpired()) {
            synchronized (election) {
                ++election.voteCount;
                if (election.isDone()) {
                    stateObject.state = State.LEADER;
                }
            }
        }
    }

    public void close() throws IOException {
        if (!isOpen) {
            return;
        }

        isOpen = false;
        for (Client client : clients) {
            client.close();
        }

        server.close();
    }

    public void setLeader(final String leaderId) {
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
