package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record AppendEntryV2(
        MessageType type,
        long term,
        String name,
        long prevLogId,
        long prevLogTerm,
        List<RaftLogHandler.LogEntry> entries
) implements Message {

    public AppendEntryV2(
            long term,
            String name,
            long prevLogIndex,
            long prevLogTerm,
            List<RaftLogHandler.LogEntry> entries
    ) {
        this(MessageType.APPEND_ENTRY_V2, term, name, prevLogIndex, prevLogTerm, entries);
    }

    @Override
    public byte[] serialize() {
        final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        ByteBuffer entryBytesBuffer = ByteBuffer.allocate(1024);
        for (final RaftLogHandler.LogEntry logEntry : entries) {
            final byte[] bytes = logEntry.serialize();
            if (entryBytesBuffer.position() + bytes.length > entryBytesBuffer.capacity()) {
                final ByteBuffer newEntryBytesBuffer = ByteBuffer.allocate(entryBytesBuffer.capacity() + bytes.length);
                newEntryBytesBuffer.put(entryBytesBuffer);
                entryBytesBuffer = newEntryBytesBuffer;
            }
            entryBytesBuffer.put(bytes);
        }

        final ByteBuffer buffer = ByteBuffer.allocate(
                4 +
                        8 +
                        4 +
                        nameBytes.length +
                        8 +
                        8 +
                        4 +
                        entryBytesBuffer.array().length
        );

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        buffer.putLong(prevLogId);
        buffer.putLong(prevLogTerm);
        buffer.putInt(entryBytesBuffer.array().length);
        buffer.put(entryBytesBuffer.array());

        return buffer.array();
    }

    public static AppendEntryV2 deserialize(final ByteBuffer buffer) {
        try {

            final int typeValue = buffer.getInt();
            final MessageType type = MessageType.fromValue(typeValue);

            final long term = buffer.getLong();

            final int nameLength = buffer.getInt();
            final byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            final String name = new String(nameBytes, StandardCharsets.UTF_8);

            final long prevLogIndex = buffer.getLong();
            final long prevLogTerm = buffer.getLong();

            final int entriesBytesLength = buffer.getInt();
            final byte[] entriesBytes = new byte[entriesBytesLength];
            buffer.get(entriesBytes);

            final ByteBuffer entriesBuffer = ByteBuffer.wrap(entriesBytes);
            final List<RaftLogHandler.LogEntry> entries = new ArrayList<>();

            while (entriesBuffer.hasRemaining()) {
                entries.add(RaftLogHandler.LogEntry.deserialize(entriesBuffer));
            }

            return new AppendEntryV2(
                    type,
                    term,
                    name,
                    prevLogIndex,
                    prevLogTerm,
                    entries
            );
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
