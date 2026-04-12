package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogEntryDeserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RaftSnapshotBodyDeserializer implements Snapshot.BodyDeserializer {

    static {
        SnapshotRegistry.registerBodyDeserializer(RaftSnapshotBody.class, new RaftSnapshotBodyDeserializer());
    }

    @Override
    public RaftSnapshotBody deserialize(final ByteBuffer buffer) {
        final RaftLogEntryDeserializer deserializer = new RaftLogEntryDeserializer();
        final long numEntries = buffer.getLong();
        final List<RaftLogEntry> entries = new ArrayList<>();

        for (int i = 0; i < numEntries; i++) {
            entries.add(deserializer.deserialize(buffer));
        }

        return new RaftSnapshotBody(entries);
    }
}
