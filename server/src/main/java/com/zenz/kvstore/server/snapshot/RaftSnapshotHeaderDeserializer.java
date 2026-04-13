package com.zenz.kvstore.server.snapshot;

import java.nio.ByteBuffer;

public class RaftSnapshotHeaderDeserializer implements SnapshotHeaderDeserializer {

    @Override
    public RaftSnapshotHeader deserialize(final ByteBuffer buffer) {
        final long version = buffer.getLong();
        final long firstLogId = buffer.getLong();
        final long firstLogTerm = buffer.getLong();
        final long lastLogId = buffer.getLong();
        final long lastLogTerm = buffer.getLong();

        return new RaftSnapshotHeader(version, firstLogId, firstLogTerm, lastLogId, lastLogTerm);
    }
}
