package com.zenz.kvstore.server.raft.message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public record InstallSnapshot(
        RaftMessageType type,
        String leaderId,
        long term,
        long lastIncludedId,
        long lastIncludedTerm,
        int offset,
        byte[] data,
        boolean done) implements Message {

    public InstallSnapshot(String leaderId,
                           long term,
                           long lastIncludedId,
                           long lastIncludedTerm,
                           int offset,
                           byte[] data,
                           boolean done) {
        this(RaftMessageType.INSTALL_SNAPSHOT, leaderId, term, lastIncludedId, lastIncludedTerm, offset, data, done);
    }

    @Override
    public byte[] serialize() {
        final byte[] leaderBytes = leaderId.getBytes(StandardCharsets.UTF_8);

        final int size =
                4 + // type
                        4 + leaderBytes.length +
                        8 + // term
                        8 + // lastIncludedId
                        8 + // lastIncludedTerm
                        8 + // offset
                        4 + data.length +
                        1;  // done

        final ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(type.getValue());

        buffer.putInt(leaderBytes.length);
        buffer.put(leaderBytes);

        buffer.putLong(term);
        buffer.putLong(lastIncludedId);
        buffer.putLong(lastIncludedTerm);
        buffer.putInt(offset);

        buffer.putInt(data.length);
        buffer.put(data);

        buffer.put((byte) (done ? 1 : 0));

        return buffer.array();
    }

    public static InstallSnapshot deserialize(final ByteBuffer buffer) {
        final RaftMessageType type = RaftMessageType.fromValue(buffer.getInt());

        final int leaderLen = buffer.getInt();
        final byte[] leaderBytes = new byte[leaderLen];
        buffer.get(leaderBytes);
        final String leaderId = new String(leaderBytes, StandardCharsets.UTF_8);

        final long term = buffer.getLong();
        final long lastIncludedId = buffer.getLong();
        final long lastIncludedTerm = buffer.getLong();
        final int offset = buffer.getInt();

        final int dataLength = buffer.getInt();
        final byte[] data = new byte[dataLength];
        buffer.get(data);

        final boolean done = buffer.get() == 1;

        return new InstallSnapshot(
                leaderId,
                term,
                lastIncludedId,
                lastIncludedTerm,
                offset,
                data,
                done
        );
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" +
                "type=" + type +
                ", leaderId='" + leaderId + '\'' +
                ", term=" + term +
                ", lastIncludedId=" + lastIncludedId +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", offset=" + offset +
                ", data=" +
                "[length=" + data.length +
                ", data=" + Arrays.toString(data) +
                "]" +
                ", done=" + done +
                '}';
    }
}