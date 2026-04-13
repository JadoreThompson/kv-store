package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.logging.LogEntryDeserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SingleSnapshotBodyDeserializer implements SnapshotBodyDeserializer {

    @Override
    public SingleSnapshotBody deserialize(final ByteBuffer buffer) {
        final LogEntryDeserializer deserializer = new LogEntryDeserializer();
        final int numEntries = buffer.getInt();
        final List<LogEntry> entries = new ArrayList<>();

        for (int i = 0; i < numEntries; i++) {
            final int len = buffer.getInt();
            final byte[] entryBytes = new byte[len];
            buffer.get(entryBytes);
            entries.add(deserializer.deserialize(ByteBuffer.wrap(entryBytes)));
        }

        return new SingleSnapshotBody(entries);
    }
}
