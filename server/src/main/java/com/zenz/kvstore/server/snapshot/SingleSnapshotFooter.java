package com.zenz.kvstore.server.snapshot;

import java.nio.ByteBuffer;


public class SingleSnapshotFooter extends Snapshot.Footer {

    public SingleSnapshotFooter(final long timestamp) {
        super(timestamp);
    }

    @Override
    public byte[] serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(timestamp);
        return buffer.array();
    }
}