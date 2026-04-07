package com.zenz.kvstore.server.command.handler;

import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.commands.GetCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.response.DeleteResponse;
import com.zenz.kvstore.common.response.ErrorResponse;
import com.zenz.kvstore.common.response.GetResponse;
import com.zenz.kvstore.common.response.PutResponse;
import com.zenz.kvstore.server.KVMap;
import com.zenz.kvstore.server.KVStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class CommandHandler implements BaseCommandHandler {
    private final KVStore store;

    public CommandHandler(KVStore store) {
        this.store = store;
    }

    @Override
    public ByteBuffer handleCommand(SocketChannel channel, Command command) {
        return handleCommand(command);
    }

    @Override
    public ByteBuffer handleCommand(Command command) {
        try {
            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
                return ByteBuffer.wrap(new PutResponse().serialize());
            }

            if (command.type().equals(CommandType.GET)) {
                GetCommand comm = (GetCommand) command;
                KVMap.Node node = store.get(comm.key());

                if (node == null) {
                    return ByteBuffer.wrap(new GetResponse(null).serialize());
                }

                return ByteBuffer.wrap(new GetResponse(node.value()).serialize());
            }

            if (command.type().equals(CommandType.DELETE)) {
                DeleteCommand comm = (DeleteCommand) command;
                boolean success = store.delete(comm.key());
                return ByteBuffer.wrap(new DeleteResponse(success).serialize());
            }

            return ByteBuffer.wrap(new ErrorResponse(
                    ErrorType.UNSUPPORTED_OPERATION, "Unknown command type: " + command.type()
            ).serialize());

        } catch (IOException e) {
            return ByteBuffer.wrap(new ErrorResponse(ErrorType.SERVER_ERROR, "IO Exception: " + e.getMessage()).serialize());
        }
    }

    public KVStore getStore() {
        return store;
    }
}
