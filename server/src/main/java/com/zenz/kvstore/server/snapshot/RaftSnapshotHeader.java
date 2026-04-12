package com.zenz.kvstore.server.snapshot;

import lombok.Getter;

import java.nio.ByteBuffer;


@Getter
public class RaftSnapshotHeader extends Snapshot.Header {

    private final long firstLogTerm;
    private final long lastLogTerm;

    public RaftSnapshotHeader(
            final long version,
            final long firstLogId,
            final long firstLogTerm,
            final long lastLogId,
            final long lastLogTerm) {
        super(version, firstLogId, lastLogId);
        this.firstLogTerm = firstLogTerm;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public byte[] serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 8 + 8 + 8);

        buffer.putLong(version);
        buffer.putLong(firstLogId);
        buffer.putLong(firstLogTerm);
        buffer.putLong(lastLogId);
        buffer.putLong(lastLogTerm);

        return buffer.array();
    }
}