package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
public class RaftSnapshotBody implements SnapshotBody {

    private final List<RaftLogEntry> entries;

    public RaftSnapshotBody(final List<RaftLogEntry> entries) {
        this.entries = entries;
    }

    @Override
    public byte[] serialize() {
        final List<byte[]> serializedEntries = entries.stream().map(RaftLogEntry::serialize).toList();
        final ByteBuffer buffer = ByteBuffer.allocate(4 + serializedEntries.size());

        buffer.putInt(serializedEntries.size());
        for (byte[] serializedEntry : serializedEntries) {
            buffer.put(serializedEntry);
        }

        return buffer.array();
    }
}
