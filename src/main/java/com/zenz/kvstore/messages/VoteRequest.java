package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

public record VoteRequest(MessageType type, long term, long logId, long brokerId) implements Message {

    public VoteRequest(long term, long logId, long brokerId) {
        this(MessageType.VOTE_REQUEST, term, logId, brokerId);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 8);

        buffer.putInt(type().getValue());
        buffer.putLong(term);
        buffer.putLong(logId);
        buffer.putLong(brokerId);

        return buffer.array();
    }

    public static VoteRequest deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        MessageType type = MessageType.fromValue(typeValue);
        if (!type.equals(MessageType.VOTE_REQUEST))
            throw new IllegalArgumentException("Invalid message type " + type.name());

        long term = buffer.getLong();
        long logId = buffer.getLong();
        long brokerId = buffer.getLong();

        return new VoteRequest(term, logId, brokerId);
    }
}
