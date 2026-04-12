package com.zenz.kvstore.server.raft.message;

import java.nio.ByteBuffer;

public record InstallSnapshotResponse(RaftMessageType type, long term) implements Message {

    public InstallSnapshotResponse(long term) {
        this(RaftMessageType.INSTALL_SNAPSHOT_RESPONSE, term);
    }

    @Override
    public byte[] serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(
                4 + // type
                        8   // term
        );

        buffer.putInt(type.getValue());
        buffer.putLong(term);

        return buffer.array();
    }

    public static InstallSnapshotResponse deserialize(final ByteBuffer buffer) {
        final RaftMessageType type = RaftMessageType.fromValue(buffer.getInt());
        final long term = buffer.getLong();

        return new InstallSnapshotResponse(term);
    }
}
