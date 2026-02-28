package com.zenz.kvstore.commandHandlers;

import com.zenz.kvstore.CommandType;
import com.zenz.kvstore.KVMap;
import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.GetCommand;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.raft.RaftController;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RaftCommandHandler implements BaseCommandHandler {
    private final KVStore store;
    private final RaftController controller;

    public RaftCommandHandler(KVStore store, RaftController controller) {
        this.store = store;
        this.controller = controller;
    }

    public ByteBuffer handleCommand(Command command) {
        try {
            CommandType commandType = command.type();
            if (commandType.equals(CommandType.GET)) {
                GetCommand comm = (GetCommand) command;
                KVMap.Node node = store.get(comm.key());
                ByteBuffer buffer = ByteBuffer.allocate(3 + node.value.length);
                buffer.put("OK ".getBytes(StandardCharsets.UTF_8));
                buffer.put(node.value);
                buffer.flip();
                return buffer;
            }

            CompletableFuture<Boolean> fut = new CompletableFuture<Boolean>();
            controller.handleCommand(command, fut);
            fut.get();

            if (commandType.equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
                return ByteBuffer.wrap("OK".getBytes(StandardCharsets.UTF_8));
            }

            return ByteBuffer.wrap("ERROR Unsupported command".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            return ByteBuffer.wrap("ERROR IO exception".getBytes(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            e.printStackTrace();
            return ByteBuffer.wrap("ERROR Thread ws interrupted".getBytes(StandardCharsets.UTF_8));
        } catch (ExecutionException e) {
            e.printStackTrace();
            return ByteBuffer.wrap(
                    ("ERROR Exception awaiting controller - " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    public KVStore getStore() {
        return store;
    }

    public RaftController getController() {
        return controller;
    }
}
