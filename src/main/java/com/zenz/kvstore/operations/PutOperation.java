package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record PutOperation(int id, String key, byte[] value) implements Operation {

    @Override
    public OperationType type() {
        return OperationType.PUT;
    }

    @Override
    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        // id(4) + type(4) + keyLength(4) + key + valueLength(4) + value
        int totalSize = 4 + 4 + 4 + keyBytes.length + 4 + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(id);
        buffer.putInt(type().getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(value.length);
        buffer.put(value);
        return buffer.array();
    }

    public static PutOperation deserialize(ByteBuffer buffer) {
        int id = buffer.getInt();
        buffer.getInt(); // Skipping the type
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new PutOperation(id, key, value);
    }

    public static PutOperation fromLine(int id, String[] components) {
        String key = components[2];
        String valueStr = components[3];
        return new PutOperation(id, key, valueStr.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
        return id + " " + type().name() + " " + key + " " + new String(value, StandardCharsets.UTF_8);
    }
}
