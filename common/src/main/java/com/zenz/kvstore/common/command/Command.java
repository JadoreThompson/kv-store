package com.zenz.kvstore.common.command;

import com.zenz.kvstore.common.enums.CommandType;

import java.nio.ByteBuffer;

public interface Command {

    CommandType type();

    byte[] serialize();

    static Command deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        CommandType type = CommandType.fromValue(typeValue);

        buffer.rewind();

        return switch (type) {
            case PUT -> PutCommand.deserialize(buffer);
            case GET -> GetCommand.deserialize(buffer);
            case DELETE -> DeleteCommand.deserialize(buffer);
            default -> throw new IllegalArgumentException("Unknown command type: " + type);
        };
    }
}