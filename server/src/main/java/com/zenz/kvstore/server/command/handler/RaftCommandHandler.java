package com.zenz.kvstore.server.command.handler;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.GetCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.response.*;
import com.zenz.kvstore.server.KVMap;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.raft.Manager;
import com.zenz.kvstore.server.raft.NodeConfig;
import com.zenz.kvstore.server.raft.State;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class RaftCommandHandler implements BaseCommandHandler {

    private final Manager manager;

    public RaftCommandHandler(final Manager manager) {
        this.manager = manager;
    }

    public BaseResponse handleCommand(final Command command) {
        try {
            final KVStore store = manager.getKvstore();
            final State nodeState = manager.getStateObject().getState();

            if (nodeState.equals(State.LEADER)) {
                final CommandType commandType = command.type();
                if (commandType.equals(CommandType.GET)) {
                    GetCommand comm = (GetCommand) command;
                    KVMap.Node node = store.get(comm.key());
                    return new GetResponse(node == null ? null : node.value());
                }

                final CompletableFuture<Boolean> fut = manager.handleCommand(command);
                fut.get();

                if (commandType.equals(CommandType.PUT)) {
                    PutCommand comm = (PutCommand) command;
                    store.put(comm.key(), comm.value());
                    return new PutResponse();
                }

                if (commandType.equals(CommandType.DELETE)) {
                    DeleteCommand comm = (DeleteCommand) command;
                    boolean success = store.delete(comm.key());
                    return new DeleteResponse(success);
                }

                return new ErrorResponse(ErrorType.UNSUPPORTED_OPERATION, null);
            }

            if (nodeState.equals(State.CANDIDATE)) {
                return new ErrorResponse(ErrorType.IN_ELECTION, null);
            }

            final String leaderId = manager.getStateObject().getLeaderId();
            if (leaderId == null) {
                return new ErrorResponse(ErrorType.LEADER_UNKNOWN, null);
            }
            NodeConfig leaderConfig = null;
            for (NodeConfig peerConfig : manager.getPeerConfigs()) {
                if (peerConfig.id().equals(leaderId)) {
                    leaderConfig = peerConfig;
                }
            }
            if (leaderConfig == null) {
                return new ErrorResponse(ErrorType.LEADER_UNKNOWN, null);
            }
            return new RedirectResponse(leaderConfig.address());
        } catch (IOException e) {
            logException(e);
            return new ErrorResponse(ErrorType.SERVER_ERROR, "IO Exception: " + e.getMessage());
        } catch (InterruptedException e) {
            logException(e);
            return new ErrorResponse(ErrorType.SERVER_ERROR, "Thread was interrupted: " + e.getMessage());
        } catch (ExecutionException e) {
            logException(e);
            return new ErrorResponse(
                    ErrorType.SERVER_ERROR,
                    "Exception awaiting controller: " + e.getMessage());
        }
    }

    private void logException(final Exception e) {
        log.error("An error occurred {}, {}", e.getClass().getName(), e.getMessage());
        log.error("Stack trace:", e);
    }
}
