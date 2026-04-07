package com.zenz.kvstore.common.command;

import com.zenz.kvstore.common.enums.CommandType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record PutCommand(String key, byte[] value) implements Command {

    public CommandType type() {
        return CommandType.PUT;
    }

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + keyBytes.length + 4 + value.length);

        buffer.putInt(type().getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(value.length);
        buffer.put(value);

        return buffer.array();
    }

    static PutCommand deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        CommandType type = CommandType.fromValue(typeValue);
        if (!type.equals(CommandType.PUT))
            throw new IllegalArgumentException("Unknown command errorType: " + typeValue);

        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        int valueLength = buffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        buffer.get(valueBytes);

        return new PutCommand(new String(keyBytes, StandardCharsets.UTF_8), valueBytes);
    }
}