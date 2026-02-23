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
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        buffer.putLong(id);
        buffer.putLong(term);
        buffer.put(key.getBytes());
        buffer.compact();

        return buffer.array();
    }

    public static RaftGetOperation deserialize(ByteBuffer buffer) {
        long id = buffer.getLong();
        long term = buffer.getLong();
        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);
        String key = new String(keyBytes);

        return new RaftGetOperation(id, term, key);
    }
}
