package com.zenz.kvstore.common.command;

import com.zenz.kvstore.common.enums.CommandType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record SearchCommand(CommandType type, String prefix) implements Command {

    public SearchCommand(String prefix) {
        this(CommandType.SEARCH, prefix);
    }

    @Override
    public byte[] serialize() {
        final byte[] keyBytes = prefix.getBytes();
        final ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + keyBytes.length);

        buffer.putInt(type.getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        return buffer.array();
    }

    public static SearchCommand deserialize(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final CommandType type = CommandType.fromValue(buffer.getInt());
        if (type != CommandType.SEARCH) {
            throw new IllegalArgumentException("Invalid command type: " + type);
        }

        final int keyLength = buffer.getInt();
        final byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        return new SearchCommand(new String(keyBytes, StandardCharsets.UTF_8));
    }
}
