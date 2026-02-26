package com.zenz.kvstore.command_handlers;

import com.zenz.kvstore.CommandType;
import com.zenz.kvstore.KVMap;
import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.GetCommand;
import com.zenz.kvstore.commands.PutCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CommandHandler implements BaseCommandHandler {
    private final KVStore store;

    public CommandHandler(KVStore store) {
        this.store = store;
    }

    @Override
    public ByteBuffer handleCommand(Command command) {
        try {
            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
                return ByteBuffer.wrap("OK".getBytes(StandardCharsets.UTF_8));
            }

            if (command.type().equals(CommandType.GET)) {
                GetCommand comm = (GetCommand) command;
                KVMap.Node node = store.get(comm.key());

                if (node == null) {
                    return ByteBuffer.wrap("NULL".getBytes(StandardCharsets.UTF_8));
                }

                ByteBuffer buffer = ByteBuffer.allocate(3 + node.value.length);
                buffer.put("OK ".getBytes(StandardCharsets.UTF_8));
                buffer.put(node.value);
                buffer.flip();
                return buffer;
            }

            return ByteBuffer.wrap(("Unknown command type: " + command).getBytes(StandardCharsets.UTF_8));

        } catch (IOException e) {
            return ByteBuffer.wrap(("ERROR " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    public KVStore getStore() {
        return store;
    }
}
