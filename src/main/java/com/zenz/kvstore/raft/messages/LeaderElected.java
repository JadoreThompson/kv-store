package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record LeaderElected(MessageType type, long term, long leaderId) implements BaseMessage {

    public LeaderElected(long term, long leaderId) {
        this(MessageType.LEADER_ELECTED, term, leaderId);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8);

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putLong(leaderId);

        return buffer.array();
    }

    public static LeaderElected deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.LEADER_ELECTED)) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            long term = buffer.getLong();
            long leaderId = buffer.getLong();

            return new LeaderElected(messageType, term, leaderId);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}