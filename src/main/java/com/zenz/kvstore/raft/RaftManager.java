package com.zenz.kvstore.raft;

import com.zenz.kvstore.raft.messages.RequestVoteResponse;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RaftBroker {
    private ExecutorService executor;
    private RaftControllerClient controllerClient;
    private RaftBrokerServer brokerServer;
    private RaftBrokerClient[] brokerClients;
    private boolean isRunning;
    private int majority;
    private int count;
    private boolean isLeader;

    public RaftBroker(
            RaftControllerClient controllerClient,
            RaftBrokerServer brokerServer,
            RaftBrokerClient[] brokerClients
    ) {
        this.controllerClient = controllerClient;
        this.brokerClients = brokerClients;
        this.brokerServer = brokerServer;
    }

    public void start() {
        isRunning = true;

        executor = Executors.newFixedThreadPool(brokerClients.length + 2);

        executor.submit(() -> wrapper(brokerServer::start));

        for (int i = 0; i < brokerClients.length; i++) {
            RaftBrokerClient brokerClient = brokerClients[i];
            executor.submit(() -> wrapper(brokerClient::start));
        }

        executor.submit(() -> wrapper(controllerClient::start));
    }

    private void wrapper(CheckedRunnable r) {
        try {
            r.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
    }

    public void join() {
    }

    /**
     * Handles the full election process when a controller crashes.
     */
    public void handleElection(RaftControllerClient controllerClient) {
        int count = 0;
        isLeader = false;
        long currentLogId = controllerClient.getLogHandler().getLogId();
        long currentTerm = controllerClient.getLogHandler().getTerm();

        for (RaftBrokerClient brokerClient : brokerClients) {
            if (brokerClient.isRunning()) {
                count++;
                brokerClient.sendVoteRequest(currentLogId, currentTerm);
            }
        }

        // Vote for ourself
        majority = count / 2 + 1;
        this.count = 1;
    }

    private void handleVote(RequestVoteResponse response) {
        if (!response.voteGranted()) return;

        count++;
        if (count >= majority) {
            if (isLeader) {

            } else {

            }

            count = 0;
            majority = 0;
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    interface CheckedRunnable {
        void run() throws Exception;
    }
}
