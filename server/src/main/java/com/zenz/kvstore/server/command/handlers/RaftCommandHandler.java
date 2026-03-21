package com.zenz.kvstore.server.command.handlers;

import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.common.commands.GetCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.responses.*;
import com.zenz.kvstore.server.KVMap;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.raft.*;
import com.zenz.kvstore.server.raft.server.handlers.RaftControllerServerHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RaftCommandHandler implements BaseCommandHandler {
    private final KVStore store;
    private final RaftManager manager;

    public RaftCommandHandler(KVStore store, RaftManager manager) {
        this.store = store;
        this.manager = manager;
    }

    public ByteBuffer handleCommand(SocketChannel channel, Command command) {
        try {
            NodeRole nodeState = manager.getRole();

            if (nodeState.equals(NodeRole.CONTROLLER)) {
                RaftControllerServerHandler controller = manager.getControllerServerHandler();

                CommandType commandType = command.type();
                if (commandType.equals(CommandType.GET)) {
                    GetCommand comm = (GetCommand) command;
                    KVMap.Node node = store.get(comm.key());
                    return ByteBuffer.wrap(new GetResponse(node.value()).serialize());
                }

                CompletableFuture<Boolean> fut = new CompletableFuture<>();
                controller.handleCommand(command, fut);
                fut.get();

                if (commandType.equals(CommandType.PUT)) {
                    PutCommand comm = (PutCommand) command;
                    store.put(comm.key(), comm.value());
                    return ByteBuffer.wrap(new PutResponse().serialize());
                } else if (commandType.equals(CommandType.DELETE)) {
                    DeleteCommand comm = (DeleteCommand) command;
                    boolean success = store.delete(comm.key());
                    return ByteBuffer.wrap(new DeleteResponse(success).serialize());
                }

                return ByteBuffer.wrap(new ErrorResponse(ErrorType.UNSUPPORTED_OPERATION, null).serialize());
            }

            if (nodeState.equals(NodeRole.CANDIDATE)) {
                return ByteBuffer.wrap(new ErrorResponse(ErrorType.IN_ELECTION, null).serialize());
            }

            RaftNode controllerNode = manager.getControllerNode();
            InetSocketAddress controllerAddress = controllerNode.serverAddress();

            // Ensure this command is from the controller. If it isn't then
            // we redirect the client to the controller.
            return ByteBuffer.wrap(new RedirectResponse(controllerAddress).serialize());
        } catch (IOException e) {
            e.printStackTrace();
            return ByteBuffer.wrap(
                    new ErrorResponse(ErrorType.SERVER_ERROR, "IO Exception: " + e.getMessage()).serialize()
            );
        } catch (InterruptedException e) {
            e.printStackTrace();
            return ByteBuffer.wrap(
                    new ErrorResponse(ErrorType.SERVER_ERROR, "Thread was interrupted: " + e.getMessage()).serialize()
            );
        } catch (ExecutionException e) {
            e.printStackTrace();
            return ByteBuffer.wrap(
                    new ErrorResponse(ErrorType.SERVER_ERROR, "Exception awaiting controller: " + e.getMessage()).serialize()
            );
        }
    }
}
