package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record RequestVoteResponse(MessageType type, boolean voteGranted, long term) implements Message {

    public RequestVoteResponse(boolean voteGranted, long term) {
        this(MessageType.REQUEST_VOTE_RESPONSE, voteGranted, term);
    }

    public static RequestVoteResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType type = MessageType.fromValue(typeValue);
            if (!type.equals(MessageType.REQUEST_VOTE_RESPONSE)) {
                throw new IllegalStateException("Invalid message errorType " + type);
            }

            int voteGranted = buffer.get();
            long term = buffer.getLong();

            return new RequestVoteResponse(voteGranted == 1, term);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + 8);

        buffer.putInt(type.getValue());
        buffer.put((byte) (voteGranted ? 1 : 0));
        buffer.putLong(term);

        return buffer.array();
    }

    @Override
    public String toString() {
        return "RequestVoteResponse{" +
                "type=" + type +
                ", voteGranted=" + voteGranted +
                ", term=" + term +
                '}';
    }
}
