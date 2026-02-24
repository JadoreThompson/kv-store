package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public interface RaftOperation {

    long term();

    long id();

    OperationType type();

    byte[] serialize();

    static RaftOperation deserialize(ByteBuffer buffer) {
        buffer.getLong(); // Skipping the id
        buffer.getLong(); // Skipping the term
        int typeValue = buffer.getInt();
        OperationType type = OperationType.fromValue(typeValue);
        buffer.rewind();

        return switch (type) {
            case PUT -> RaftPutOperation.deserialize(buffer);
            case GET -> RaftGetOperation.deserialize(buffer);
            default -> throw new UnsupportedOperationException("Unsupported operation " + type.getValue());
        };
    }

    public static List<RaftOperation> parseRaftLogs(ByteBuffer bb) throws IOException {
        List<RaftOperation> operations = new ArrayList<>();

        while (bb.hasRemaining()) {
            long id = bb.getLong();
            long term = bb.getLong();
            int typeValue = bb.getInt();
            OperationType operationType = OperationType.fromValue(typeValue);

            if (operationType.equals(OperationType.PUT)) {
                ByteBuffer _buffer = ByteBuffer.allocate(10240);

                _buffer.putLong(id);
                _buffer.putLong(term);
                _buffer.putInt(typeValue);

                int keyLength = bb.getInt();
                _buffer.putInt(keyLength);
                byte[] key = new byte[keyLength];
                bb.get(key);
                _buffer.put(key);

                int valueLength = bb.getInt();
                _buffer.putInt(valueLength);
                byte[] value = new byte[valueLength];
                bb.get(value);
                _buffer.put(value);

                _buffer.flip();
                RaftPutOperation operation = RaftPutOperation.deserialize(_buffer);

                // Skipping new line character
                bb.get();
                operations.add(operation);
            } else if (operationType.equals(OperationType.GET)) {
                ByteBuffer _buffer = ByteBuffer.allocate(10240);

                _buffer.putLong(id);
                _buffer.putLong(term);
                _buffer.putInt(typeValue);

                int keyLength = bb.getInt();
                _buffer.putInt(keyLength);
                byte[] key = new byte[keyLength];
                bb.get(key);
                _buffer.put(key);

                _buffer.flip();
                RaftGetOperation operation = RaftGetOperation.deserialize(_buffer);

                bb.get(); // Getting the new line character
                operations.add(operation);
            } else {
                throw new UnsupportedOperationException("Unsupported operation type " + operationType.name());
            }
        }

        return operations;
    }

    @Override
    public String toString();
}
