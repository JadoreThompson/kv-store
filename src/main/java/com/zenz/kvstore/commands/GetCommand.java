package com.zenz.kvstore.commands;

import com.zenz.kvstore.CommandType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record GetCommand(String key) implements Command {

    @Override
    public CommandType type() {
        return CommandType.GET;
    }

    @Override
    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + keyBytes.length);

        buffer.putInt(type().getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        return buffer.array();
    }

    static GetCommand deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int typeValue = buffer.getInt();
        CommandType type = CommandType.fromValue(typeValue);
        if (!type.equals(CommandType.GET)) throw new IllegalArgumentException("Unknown command type: " + typeValue);

        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        return new GetCommand(new String(keyBytes, StandardCharsets.UTF_8));
    }
}