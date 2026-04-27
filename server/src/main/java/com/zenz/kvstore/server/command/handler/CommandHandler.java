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

public class CommandHandler implements BaseCommandHandler {

    private final KVStore store;

    public CommandHandler(KVStore store) {
        this.store = store;
    }

    @Override
    public BaseResponse handleCommand(Command command) {
        try {
            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
                return new PutResponse();
            }

            if (command.type().equals(CommandType.GET)) {
                GetCommand comm = (GetCommand) command;
                KVMap.Node node = store.get(comm.key());
                return new GetResponse(node == null ? null : node.value());
            }

            if (command.type().equals(CommandType.DELETE)) {
                DeleteCommand comm = (DeleteCommand) command;
                boolean success = store.delete(comm.key());
                return new DeleteResponse(success);
            }

            return new ErrorResponse(
                    ErrorType.UNSUPPORTED_OPERATION,
                    "Unknown command type: " + command.type());

        } catch (Exception e) {
            return new ErrorResponse(ErrorType.SERVER_ERROR, "Exception: " + e.getMessage());
        }
    }
}