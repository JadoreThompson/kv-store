package com.zenz.kvstore.commands;

import com.zenz.kvstore.CommandType;

import java.nio.ByteBuffer;

public interface Command {
    CommandType type();

    byte[] serialize();

    static Command deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int typeValue = buffer.getInt();
        CommandType type = CommandType.fromValue(typeValue);

        if (type.equals(CommandType.PUT)) return PutCommand.deserialize(bytes);
        if (type.equals(CommandType.GET)) return GetCommand.deserialize(bytes);

        throw new IllegalArgumentException("Unknown command type: " + typeValue);
    }
}