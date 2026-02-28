package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record RequestEntry(MessageType type, long id, long term) implements BaseMessage {

    public RequestEntry(long id, long term) {
        this(MessageType.REQUEST_ENTRY, id, term);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8);

        buffer.putInt(type.getValue());
        buffer.putLong(id);
        buffer.putLong(term);

        return buffer.array();
    }

    public static RequestEntry deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.REQUEST_ENTRY)) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            long id = buffer.getLong();
            long term = buffer.getLong();

            return new RequestEntry(id, term);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}