package com.zenz.kvstore.server.command.handler;

import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.commands.GetCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.responses.*;
import com.zenz.kvstore.server.KVMap;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.raft.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RaftCommandHandler implements BaseCommandHandler {
    private final KVStore store;
    private RaftManager manager;
    private Manager managerNew;

    @Deprecated
    public RaftCommandHandler(KVStore store, RaftManager manager) {
        this.store = store;
        this.manager = manager;
    }

    public RaftCommandHandler(KVStore store, Manager manager) {
        this.store = store;
        this.managerNew = manager;
    }

    public ByteBuffer handleCommand(Command command) {
        try {
            NodeRole nodeState = this.managerNew.getRole();

            if (nodeState.equals(NodeRole.CONTROLLER)) {
                Server server = this.managerNew.getServer();

                CommandType commandType = command.type();
                if (commandType.equals(CommandType.GET)) {
                    GetCommand comm = (GetCommand) command;
                    KVMap.Node node = store.get(comm.key());
                    return ByteBuffer.wrap(new GetResponse(node.value()).serialize());
                }

                CompletableFuture<Boolean> fut = new CompletableFuture<>();
                server.handleCommand(command, fut);
                fut.get();

                if (commandType.equals(CommandType.PUT)) {
                    PutCommand comm = (PutCommand) command;
                    store.put(comm.key(), comm.value());
                    return ByteBuffer.wrap(new PutResponse().serialize());
                }

                if (commandType.equals(CommandType.DELETE)) {
                    DeleteCommand comm = (DeleteCommand) command;
                    boolean success = store.delete(comm.key());
                    return ByteBuffer.wrap(new DeleteResponse(success).serialize());
                }

                return ByteBuffer.wrap(new ErrorResponse(ErrorType.UNSUPPORTED_OPERATION, null).serialize());
            }

            if (nodeState.equals(NodeRole.CANDIDATE)) {
                return ByteBuffer.wrap(new ErrorResponse(ErrorType.IN_ELECTION, null).serialize());
            }

            NodeConfig controllerConfig = this.managerNew.getControllerConfig();
            if (controllerConfig == null) {
                return ByteBuffer.wrap(new ErrorResponse(ErrorType.CONTROLLER_UNKNOWN, null).serialize());
            }

            return ByteBuffer.wrap(new RedirectResponse(controllerConfig.clientAddress()).serialize());
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
