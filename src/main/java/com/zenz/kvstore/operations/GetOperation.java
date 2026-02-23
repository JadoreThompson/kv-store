package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record GetOperation(int id, String key) implements Operation {

    @Override
    public OperationType type() {
        return OperationType.GET;
    }

    @Override
    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        // id(4) + type(4) + keyLength(4) + key
        int totalSize = 4 + 4 + 4 + keyBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(id);
        buffer.putInt(type().getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        return buffer.array();
    }

    public static GetOperation deserialize(ByteBuffer buffer) {
        int id = buffer.getInt();
        buffer.getInt(); // Skipping the type
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        return new GetOperation(id, key);
    }

    public static GetOperation fromLine(int id, String[] components) {
        String key = components[2];
        return new GetOperation(id, key);
    }

    @Override
    public String toString() {
        return id + " " + type().name() + " " + key;
    }
}
