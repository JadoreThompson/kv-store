package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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

    public static List<Operation> parseRaftLogs(ByteBuffer bb) throws IOException {
        List<Operation> operations = new ArrayList<>();

        while (bb.hasRemaining()) {
            long id = bb.getLong();
            int typeValue = bb.getInt();
            OperationType operationType = OperationType.fromValue(typeValue);

            if (operationType.equals(OperationType.PUT)) {
                ByteBuffer _buffer = ByteBuffer.allocate(10240);

                _buffer.putLong(id);
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
                PutOperation operation = PutOperation.deserialize(_buffer);

                // Skipping new line character
                bb.get();
                operations.add(operation);
            } else if (operationType.equals(OperationType.GET)) {
                ByteBuffer _buffer = ByteBuffer.allocate(10240);

                _buffer.putLong(id);
                _buffer.putInt(typeValue);

                int keyLength = bb.getInt();
                _buffer.putInt(keyLength);
                byte[] key = new byte[keyLength];
                bb.get(key);
                _buffer.put(key);

                _buffer.flip();
                GetOperation operation = GetOperation.deserialize(_buffer);

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
