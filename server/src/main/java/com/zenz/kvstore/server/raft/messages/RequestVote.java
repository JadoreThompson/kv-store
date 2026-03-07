package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record RequestVote(
        MessageType type,
        long term,
        long candidateId,
        long logId,
        long prevTerm
) implements BaseMessage {

    public RequestVote(long candidateId, long term, long prevLogId, long prevTerm) {
        this(MessageType.REQUEST_VOTE, term, candidateId, prevLogId, prevTerm);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 8 + 8);

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putLong(candidateId);
        buffer.putLong(logId);
        buffer.putLong(prevTerm);

        return buffer.array();
    }

    public static RequestVote deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.REQUEST_VOTE)) {
                throw new IllegalArgumentException("Invalid message errorType " + messageType);
            }

            long term = buffer.getLong();
            long candidateId = buffer.getLong();
            long prevLogId = buffer.getLong();
            long prevTerm = buffer.getLong();

            return new RequestVote(messageType, term, candidateId, prevLogId, prevTerm);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "RequestVote{" +
                "errorType=" + type +
                ", term=" + term +
                ", candidateId=" + candidateId +
                ", logId=" + logId +
                ", prevTerm=" + prevTerm +
                '}';
    }
}