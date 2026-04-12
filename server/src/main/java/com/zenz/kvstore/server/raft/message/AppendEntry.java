package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.util.KVSerializable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record AppendEntry(
        RaftMessageType type,
        String leaderId,
        long term,
        long prevLogId,
        long prevLogTerm,
        List<RaftLogEntry> entries) implements Message {

    public AppendEntry(
            final String leaderId,
            final long term,
            final long prevLogId,
            final long prevLogTerm,
            final List<RaftLogEntry> entries) {
        this(RaftMessageType.APPEND_ENTRY, leaderId, term, prevLogId, prevLogTerm, entries);
    }

    @Override
    public byte[] serialize() {
        final byte[] leaderBytes = leaderId.getBytes(StandardCharsets.UTF_8);

        int size =
                4 + // type
                        4 + leaderBytes.length +
                        8 + // term
                        8 + // prevLogId
                        8 + // prevLogTerm
                        4; // entries count

        final List<byte[]> serializedEntries = new ArrayList<>();
        for (RaftLogEntry entry : entries) {
            byte[] bytes = entry.serialize();
            serializedEntries.add(bytes);
            size += 4 + bytes.length; // length + data
        }

        final ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(type.getValue());
        buffer.putInt(leaderBytes.length);
        buffer.put(leaderBytes);
        buffer.putLong(term);
        buffer.putLong(prevLogId);
        buffer.putLong(prevLogTerm);

        buffer.putInt(entries.size());
        for (byte[] entryBytes : serializedEntries) {
            buffer.putInt(entryBytes.length);
            buffer.put(entryBytes);
        }

        return buffer.array();
    }

    public static AppendEntry deserialize(final ByteBuffer buffer) {
        final RaftMessageType type = RaftMessageType.fromValue(buffer.getInt());

        final int leaderLen = buffer.getInt();
        final byte[] leaderBytes = new byte[leaderLen];
        buffer.get(leaderBytes);
        final String leaderId = new String(leaderBytes, StandardCharsets.UTF_8);

        final long term = buffer.getLong();
        final long prevLogId = buffer.getLong();
        final long prevLogTerm = buffer.getLong();

        final int entryCount = buffer.getInt();
        final List<RaftLogEntry> entries = new ArrayList<>(entryCount);

        for (int i = 0; i < entryCount; i++) {
            final int len = buffer.getInt();
            final byte[] entryBytes = new byte[len];
            buffer.get(entryBytes);

            final RaftLogEntry entry =
                    RaftLogEntry.deserialize(ByteBuffer.wrap(entryBytes));

            entries.add(entry);
        }

        return new AppendEntry(
                leaderId,
                term,
                prevLogId,
                prevLogTerm,
                entries
        );
    }
}