package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.KVStore;
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
        return null;
    }

    public Future<Void> replicateCommand(final Command command) {
        return null;
    }

    public void close() {
    }
}
