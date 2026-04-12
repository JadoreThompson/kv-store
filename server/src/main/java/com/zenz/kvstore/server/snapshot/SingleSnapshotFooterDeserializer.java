package com.zenz.kvstore.server.snapshot;

import java.nio.ByteBuffer;


public class SingleSnapshotFooterDeserializer implements Snapshot.FooterDeserializer {

    static {
        SnapshotRegistry.registerFooterDeserializer(SingleSnapshotFooter.class, new SingleSnapshotFooterDeserializer());
    }

    @Override
    public SingleSnapshotFooter deserialize(final ByteBuffer buffer) {
        final long timestamp = buffer.getLong();
        return new SingleSnapshotFooter(timestamp);
    }
}