package com.zenz.kvstore.commandHandlers;

import com.zenz.kvstore.CommandType;
import com.zenz.kvstore.KVMap;
import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.GetCommand;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.raft.RaftControllerServerHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RaftCommandHandler implements BaseCommandHandler {
    private final KVStore store;
    private final RaftControllerServerHandler controller;
    private final String DEBUG_PREFIX;

    public RaftCommandHandler(KVStore store, RaftControllerServerHandler controller) {
        this.store = store;
        this.controller = controller;
        DEBUG_PREFIX = "[RaftCommandHandler]";
    }

    @Override
    public ByteBuffer handleCommand(SocketChannel channel, Command command) {
        return handleCommand(command);
    }

    public ByteBuffer handleCommand(Command command) {
        final String debugPrefix = DEBUG_PREFIX + "[handleCommand] ";
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

    public RaftControllerServerHandler getController() {
        return controller;
    }
}
