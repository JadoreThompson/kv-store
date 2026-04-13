package com.zenz.kvstore.server.snapshot;

import java.nio.ByteBuffer;


public class SingleSnapshotFooterDeserializer implements SnapshotFooterDeserializer {

    @Override
    public SingleSnapshotFooter deserialize(final ByteBuffer buffer) {
        final long timestamp = buffer.getLong();
        return new SingleSnapshotFooter(timestamp);
    }
}