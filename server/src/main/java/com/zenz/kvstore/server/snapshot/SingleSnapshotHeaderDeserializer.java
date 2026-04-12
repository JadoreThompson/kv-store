package com.zenz.kvstore.server.snapshot;

import java.nio.ByteBuffer;

public class SingleSnapshotHeaderDeserializer implements Snapshot.HeaderDeserializer {

    static {
        SnapshotRegistry.registerHeaderDeserializer(SingleSnapshotHeader.class, new SingleSnapshotHeaderDeserializer());
    }

    @Override
    public SingleSnapshotHeader deserialize(final ByteBuffer buffer) {
        final long version = buffer.getLong();
        final long firstLogId = buffer.getLong();
        final long lastLogId = buffer.getLong();

        return new SingleSnapshotHeader(version, firstLogId, lastLogId);
    }
}
