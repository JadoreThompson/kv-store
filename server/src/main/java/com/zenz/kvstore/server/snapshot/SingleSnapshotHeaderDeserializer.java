package com.zenz.kvstore.server.snapshot;

import java.nio.ByteBuffer;

public class SingleSnapshotHeaderDeserializer implements SnapshotHeaderDeserializer {

    @Override
    public SingleSnapshotHeader deserialize(final ByteBuffer buffer) {
        final long version = buffer.getLong();
        final long firstLogId = buffer.getLong();
        final long lastLogId = buffer.getLong();

        return new SingleSnapshotHeader(version, firstLogId, lastLogId);
    }
}
