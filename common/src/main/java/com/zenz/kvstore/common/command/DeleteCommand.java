package com.zenz.kvstore.common.command;

import com.zenz.kvstore.common.enums.CommandType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record DeleteCommand(CommandType type, String key) implements Command {

    public DeleteCommand(String key) {
        this(CommandType.DELETE, key);
    }

    @Override
    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + keyBytes.length);
        buffer.putInt(type.getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        return buffer.array();
    }

    public static DeleteCommand deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        CommandType commandType = CommandType.fromValue(typeValue);
        if (!commandType.equals(CommandType.DELETE)) {
            throw new IllegalArgumentException("Invalid command type " + commandType);
        }
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        return new DeleteCommand(key);
    }
}
