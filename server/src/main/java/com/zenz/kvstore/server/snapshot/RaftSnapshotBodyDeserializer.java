package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogEntryDeserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RaftSnapshotBodyDeserializer implements SnapshotBodyDeserializer {

    @Override
    public RaftSnapshotBody deserialize(final ByteBuffer buffer) {
        final RaftLogEntryDeserializer deserializer = new RaftLogEntryDeserializer();
        final int numEntries = buffer.getInt();
        final List<RaftLogEntry> entries = new ArrayList<>();

        for (int i = 0; i < numEntries; i++) {
            final int len = buffer.getInt();
            final byte[] entryBytes = new byte[len];
            buffer.get(entryBytes);
            entries.add(deserializer.deserialize(ByteBuffer.wrap(entryBytes)));
        }

        return new RaftSnapshotBody(entries);
    }
}
