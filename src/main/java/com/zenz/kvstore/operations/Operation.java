package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.nio.ByteBuffer;

public interface Operation {
    int id();

    OperationType type();

    byte[] serialize();

    static Operation fromLine(String line) {
        String[] components = line.strip().split(" ");
        int id = Integer.parseInt(components[0]);
        OperationType type = OperationType.valueOf(components[1]);

        return switch (type) {
            case PUT -> PutOperation.fromLine(id, components);
            case GET -> GetOperation.fromLine(id, components);
            default -> throw new UnsupportedOperationException("Unsupported operation " + type.getValue());
        };
    }

    static Operation deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        OperationType type = OperationType.fromValue(typeValue);
        buffer.rewind();

        return switch (type) {
            case PUT -> PutOperation.deserialize(buffer);
            case GET -> GetOperation.deserialize(buffer);
            default -> throw new UnsupportedOperationException("Unsupported operation " + type.getValue());
        };
    }

    @Override
    public String toString();
}
