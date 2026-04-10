package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.util.KVSerializable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record RequestVote(
        RaftMessageType type,
        String leaderName,
        long lastLogId,
        long lastLogTerm
) implements Message, KVSerializable {

    public RequestVote(String leaderName, long lastLogId, long lastLogTerm) {
        this(RaftMessageType.REQUEST_VOTE, leaderName, lastLogId, lastLogTerm);
    }

    @Override
    public byte[] serialize() {
        final byte[] leaderBytes = leaderName.getBytes(StandardCharsets.UTF_8);

        final int size =
                4 + // type
                        4 + leaderBytes.length +
                        8 + // lastLogId
                        8;  // lastLogTerm

        final ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(type.getValue());

        buffer.putInt(leaderBytes.length);
        buffer.put(leaderBytes);

        buffer.putLong(lastLogId);
        buffer.putLong(lastLogTerm);

        return buffer.array();
    }

    public static RequestVote deserialize(final ByteBuffer buffer) {
        final RaftMessageType type = RaftMessageType.fromValue(buffer.getInt());

        final int leaderLen = buffer.getInt();
        final byte[] leaderBytes = new byte[leaderLen];
        buffer.get(leaderBytes);
        final String leaderName = new String(leaderBytes, StandardCharsets.UTF_8);

        final long lastLogId = buffer.getLong();
        final long lastLogTerm = buffer.getLong();

        return new RequestVote(
                type,
                leaderName,
                lastLogId,
                lastLogTerm
        );
    }
}