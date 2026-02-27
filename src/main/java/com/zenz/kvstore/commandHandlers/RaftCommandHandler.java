package com.zenz.kvstore.commandHandlers;

import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.raft.RaftController;

import java.nio.ByteBuffer;

public class RaftCommandHandler implements BaseCommandHandler {
    private final KVStore store;
    private final RaftController controller;

    public RaftCommandHandler(KVStore store, RaftController controller) {
        this.store = store;
        this.controller = controller;
    }

    public ByteBuffer handleCommand(Command command) {
        return ByteBuffer.wrap(new byte[0]);
    }

    public KVStore getStore() {
        return store;
    }

    public RaftController getController() {
        return controller;
    }
}
