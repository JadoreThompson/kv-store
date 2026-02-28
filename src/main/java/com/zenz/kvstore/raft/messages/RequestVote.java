package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record RequestVote(MessageType type, long term, long candidateId, long lastLogIndex, long lastLogTerm) implements BaseMessage {

    public RequestVote(long term, long candidateId, long lastLogIndex, long lastLogTerm) {
        this(MessageType.REQUEST_VOTE, term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 8 + 8);

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putLong(candidateId);
        buffer.putLong(lastLogIndex);
        buffer.putLong(lastLogTerm);

        return buffer.array();
    }

    public static RequestVote deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.REQUEST_VOTE)) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            long term = buffer.getLong();
            long candidateId = buffer.getLong();
            long lastLogIndex = buffer.getLong();
            long lastLogTerm = buffer.getLong();

            return new RequestVote(messageType, term, candidateId, lastLogIndex, lastLogTerm);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}