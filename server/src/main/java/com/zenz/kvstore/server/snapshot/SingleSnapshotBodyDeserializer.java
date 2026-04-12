package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.logging.LogEntryDeserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SingleSnapshotBodyDeserializer implements Snapshot.BodyDeserializer {

    static {
        SnapshotRegistry.registerBodyDeserializer(SingleSnapshotBody.class, new SingleSnapshotBodyDeserializer());
    }

    @Override
    public SingleSnapshotBody deserialize(final ByteBuffer buffer) {
        final LogEntryDeserializer deserializer = new LogEntryDeserializer();
        final long numEntries = buffer.getLong();
        final List<LogEntry> entries = new ArrayList<>();

        for (int i = 0; i < numEntries; i++) {
            entries.add(deserializer.deserialize(buffer));
        }

        return new SingleSnapshotBody(entries);
    }
}
