package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
public class SingleSnapshotBody implements SnapshotBody {

    private final List<LogEntry> entries;

    public SingleSnapshotBody(final List<LogEntry> entries) {
        this.entries = entries;
    }

    @Override
    public byte[] serialize() {
        final List<byte[]> serializedEntries = entries.stream().map(LogEntry::serialize).toList();
        int totalSize = 4; // initial size

        for (byte[] entry : serializedEntries) {
            totalSize += 4 + entry.length;
        }

        final ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(serializedEntries.size());
        for (byte[] serializedEntry : serializedEntries) {
            buffer.putInt(serializedEntry.length);
            buffer.put(serializedEntry);
        }

        return buffer.array();
    }
}