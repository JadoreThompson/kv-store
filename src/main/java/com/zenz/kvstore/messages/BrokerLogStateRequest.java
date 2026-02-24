package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.operations.Operation;

import java.nio.ByteBuffer;

public record BrokerLogStateRequest(MessageType type, long brokerId, long id, long term,
                                    Operation operation) implements Message {
    public BrokerLogStateRequest(long brokerId, long index, long term, Operation operation) {
        this(MessageType.BROKER_LOG_STATE_REQUEST, brokerId, index, term, operation);
    }

    @Override
    public MessageType type() {
        return MessageType.BROKER_LOG_STATE_REQUEST;
    }

    public byte[] serialize() {
        byte[] opBytes = operation.serialize();
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 8 + 4 + opBytes.length);

        buffer.putInt(type().getValue());
        buffer.putLong(brokerId);
        buffer.putLong(id);
        buffer.putLong(term);
        buffer.putInt(opBytes.length);
        buffer.put(opBytes);

        return buffer.array();
    }

    public static BrokerLogStateRequest deserialize(ByteBuffer buffer) {
        int type = buffer.getInt();
        MessageType messageType = MessageType.fromValue(type);
        if (!messageType.equals(MessageType.BROKER_LOG_STATE_REQUEST))
            throw new IllegalArgumentException("Invalid message type " + messageType.name());

        long brokerId = buffer.getLong();
        long id = buffer.getLong();
        long term = buffer.getLong();
        int opLen = buffer.getInt();
        byte[] opBytes = new byte[opLen];
        buffer.get(opBytes);
        Operation operation = Operation.deserialize(ByteBuffer.wrap(opBytes));

        return new BrokerLogStateRequest(brokerId, id, term, operation);
    }
}
