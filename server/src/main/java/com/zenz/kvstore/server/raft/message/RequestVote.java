package com.zenz.kvstore.server.raft.message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record RequestVote(
        RaftMessageType type,
        String leaderId,
        long term,
        long lastLogId,
        long lastLogTerm) implements Message {

    public RequestVote(final String leaderId, final long term, final long lastLogId, final long lastLogTerm) {
        this(RaftMessageType.REQUEST_VOTE, leaderId, term, lastLogId, lastLogTerm);
    }

    @Override
    public byte[] serialize() {
        final byte[] leaderBytes = leaderId.getBytes(StandardCharsets.UTF_8);

        final int size =
                4 + // type
                        4 + leaderBytes.length +
                        8 + // term
                        8 + // lastLogId
                        8;  // lastLogTerm

        final ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(type.getValue());

        buffer.putInt(leaderBytes.length);
        buffer.put(leaderBytes);

        buffer.putLong(term);
        buffer.putLong(lastLogId);
        buffer.putLong(lastLogTerm);

        return buffer.array();
    }

    public static RequestVote deserialize(final ByteBuffer buffer) {
        final RaftMessageType type = RaftMessageType.fromValue(buffer.getInt());

        final int leaderLen = buffer.getInt();
        final byte[] leaderBytes = new byte[leaderLen];
        buffer.get(leaderBytes);
        final String leaderId = new String(leaderBytes, StandardCharsets.UTF_8);
        final long term = buffer.getLong();
        final long lastLogId = buffer.getLong();
        final long lastLogTerm = buffer.getLong();

        return new RequestVote(
                leaderId,
                term,
                lastLogId,
                lastLogTerm
        );
    }

    @Override
    public String toString() {
        return "RequestVote{" +
                "type=" + type +
                ", leaderId='" + leaderId + '\'' +
                ", term=" + term +
                ", lastLogId=" + lastLogId +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}