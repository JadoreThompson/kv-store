package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record AppendEntryResponse(
        MessageType type,
        long id,
        long term,
        boolean success
) implements Message {

    public AppendEntryResponse(long id, long term, boolean success) {
        this(MessageType.APPEND_ENTRY_RESPONSE, id, term, success);
    }

    public static AppendEntryResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.APPEND_ENTRY_RESPONSE)) {
                throw new IllegalArgumentException("Invalid message errorType " + messageType);
            }

            long id = buffer.getLong();
            long term = buffer.getLong();
            boolean success = buffer.get() == 1;

            return new AppendEntryResponse(messageType, id, term, success);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 1);

        buffer.putInt(type.getValue());
        buffer.putLong(id);
        buffer.putLong(term);
        buffer.put((byte) (success ? 1 : 0));

        return buffer.array();
    }

    @Override
    public String toString() {
        return "AppendEntryResponse{" +
                "type=" + type +
                ", id=" + id +
                ", term=" + term +
                ", success=" + success +
                '}';
    }
}
