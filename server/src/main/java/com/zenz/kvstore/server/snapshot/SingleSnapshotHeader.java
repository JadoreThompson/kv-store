package com.zenz.kvstore.server.snapshot;

import lombok.Getter;

import java.nio.ByteBuffer;


@Getter
public class SingleSnapshotHeader extends SnapshotHeader {

    public SingleSnapshotHeader(
            final long version,
            final long firstLogId,
            final long lastLogId) {
        super(version, firstLogId, lastLogId);
    }

    @Override
    public byte[] serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 8);

        buffer.putLong(version);
        buffer.putLong(firstLogId);
        buffer.putLong(lastLogId);

        return buffer.array();
    }
}