package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
public class SingleSnapshotBody implements Snapshot.Body {

    private final List<LogEntry> entries;

    public SingleSnapshotBody(final List<LogEntry> entries) {
        this.entries = entries;
    }

    @Override
    public byte[] serialize() {
        final List<byte[]> serializedEntries = entries.stream().map(LogEntry::serialize).toList();
        final ByteBuffer buffer = ByteBuffer.allocate(4 + serializedEntries.size());

        buffer.putInt(serializedEntries.size());
        for (byte[] serializedEntry : serializedEntries) {
            buffer.put(serializedEntry);
        }

        return buffer.array();
    }
}