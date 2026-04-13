package com.zenz.kvstore.server.snapshot;

import java.nio.ByteBuffer;


public class RaftSnapshotFooterDeserializer implements SnapshotFooterDeserializer {

    @Override
    public RaftSnapshotFooter deserialize(final ByteBuffer buffer) {
        final long timestamp = buffer.getLong();
        return new RaftSnapshotFooter(timestamp);
    }
}