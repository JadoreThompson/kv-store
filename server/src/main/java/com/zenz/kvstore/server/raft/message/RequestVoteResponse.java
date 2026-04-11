package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.util.KVSerializable;

import java.nio.ByteBuffer;

public record RequestVoteResponse(
        RaftMessageType type,
        long term,
        boolean voteGranted) implements Message, KVSerializable {

    public RequestVoteResponse(long term, boolean voteGranted) {
        this(RaftMessageType.REQUEST_VOTE, term, voteGranted);
    }

    @Override
    public byte[] serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(
                4 + // type
                        8 + // term
                        1   // voteGranted
        );

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.put((byte) (voteGranted ? 1 : 0));

        return buffer.array();
    }

    public static RequestVoteResponse deserialize(final ByteBuffer buffer) {
        final RaftMessageType type = RaftMessageType.fromValue(buffer.getInt());
        final long term = buffer.getLong();
        final boolean voteGranted = buffer.get() == 1;

        return new RequestVoteResponse(term, voteGranted);
    }
}