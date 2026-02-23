package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface RaftOperation {

    long term();

    long id();

    OperationType type();

    byte[] serialize();

//    static com.zenz.kvstore.operations.Operation fromLine(String line) {
//        String[] components = line.strip().split(" ");
//        long term = Long.parseLong(components[0]);
//        long id = Long.parseLong(components[1]);
//        OperationType type = OperationType.valueOf(components[1]);
//
//        return switch (type) {
//            case PUT -> RaftPutOperation.fromLine(components);
//            case GET -> RaftGetOperation.fromLine( omponents);
//            default -> throw new UnsupportedOperationException("Unsupported operation " + type.getValue());
//        };
//    }

    static RaftOperation deserialize(ByteBuffer buffer) {
        buffer.getLong(); // Skipping the id
        buffer.getLong(); // Skipping the term
        int typeValue = buffer.getInt();
        OperationType type = OperationType.fromValue(typeValue);
        buffer.rewind();

        System.out.println(new String(buffer.array(), StandardCharsets.UTF_8));
//        for (byte b : buffer.array()) {
//            if (b == 0) continue;
//            System.out.print((char) b);
//        }

        return switch (type) {
            case PUT -> RaftPutOperation.deserialize(buffer);
            case GET -> RaftGetOperation.deserialize(buffer);
            default -> throw new UnsupportedOperationException("Unsupported operation " + type.getValue());
        };
    }

    @Override
    public String toString();
}
