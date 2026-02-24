package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.nio.ByteBuffer;

public record RaftGetOperation(long id, long term, String key) implements RaftOperation {

    @Override
    public OperationType type() {
        return OperationType.GET;
    }

    @Override
    public byte[] serialize() {
        byte[] keyBytes = key.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 4 + 4 + keyBytes.length);

        buffer.putLong(id);
        buffer.putLong(term);
        buffer.putInt(type().getValue());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        return buffer.array();
    }

    public static RaftGetOperation deserialize(ByteBuffer buffer) {
        long id = buffer.getLong();
        long term = buffer.getLong();
        buffer.getInt(); // Skipping the type
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes);

        return new RaftGetOperation(id, term, key);
    }

    @Override
    public String toString() {
        return id + " " + term + " " + type().name() + " " + key;
    }
}
