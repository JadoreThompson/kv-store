package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record RaftPutOperation(long id, long term, String key, byte[] value) implements RaftOperation {

    @Override
    public OperationType type() {
        return OperationType.PUT;
    }

    @Override
    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 4 + 4 + keyBytes.length + 4 + value.length);

        buffer.putLong(id);
        buffer.putLong(term);
        buffer.putInt(type().getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(value.length);
        buffer.put(value);

        return buffer.array();
    }

    public static RaftPutOperation deserialize(ByteBuffer buffer) {
        System.out.println("Deserializing a buffer of length " + buffer.array().length);
        long id = buffer.getLong();
        long term = buffer.getLong();
        buffer.getInt(); // Skipping the type

        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);

        return new RaftPutOperation(id, term, key, value);
    }

    @Override
    public String toString() {
        return id + " " + type().name() + " " + key + " " + new String(value, StandardCharsets.UTF_8);
    }
}
