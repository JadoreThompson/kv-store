package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.raft.message.RequestVoteResponse;
import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;


public class Manager implements Closeable {

    @Getter
    @Setter
    private KVStore kvstore;

    @Getter
    private final NodeConfig nodeConfig;

    @Getter
    private final List<NodeConfig> peerConfigs = new ArrayList<>();

    private Server server;
    private final List<Client> clients = new ArrayList<>();
    private StateObject stateObject;

    public Manager(final KVStore kvStore, final NodeConfig nodeConfig, final List<NodeConfig> peerConfigs) {
        this.kvstore = kvStore;
        this.nodeConfig = nodeConfig;
        this.peerConfigs.addAll(peerConfigs);
    }

    public void open() {
    }

    private void watch() {
    }

    public StateObject.Election startElection() {
        final int majority = (1 + peerConfigs.size()) / 2;
        final long term = stateObject.setCurrentTerm(stateObject.getCurrentTerm() + 1);
        stateObject.election = new StateObject.Election(term, majority, System.currentTimeMillis() + 1500);
        return stateObject.election;
    }

    public void handleRequestVoteResponse(final RequestVoteResponse response) {
    }

    public Future<Void> replicateCommand(final Command command) {
        return null;
    }

    public void close() {
    }
}
