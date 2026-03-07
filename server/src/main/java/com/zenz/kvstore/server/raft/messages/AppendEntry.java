package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.MessageType;
import com.zenz.kvstore.server.logHandlers.RaftLogHandler;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public record AppendEntry(
        MessageType type,
        long id,
        long term,
        List<RaftLogHandler.Log> entries
) implements BaseMessage {

    public AppendEntry(
            long id,
            long term,
            List<RaftLogHandler.Log> entries
    ) {
        this(MessageType.APPEND_ENTRY, id, term, entries);
    }

    @Override
    public byte[] serialize() {
        // Calculate total size needed for entries
        int entriesSize = 0;
        List<byte[]> entryBytesList = new ArrayList<>();

        if (entries != null) {
            for (RaftLogHandler.Log entry : entries) {
                if (entry == null) continue;

                byte[] entryBytes = entry.serialize();
                entryBytesList.add(entryBytes);
                entriesSize += 4 + entryBytes.length; // 4 bytes for length + entry bytes
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + entriesSize);

        buffer.putInt(type.getValue());
        buffer.putLong(id);
        buffer.putLong(term);
        buffer.putInt(entriesSize);

        for (byte[] entryBytes : entryBytesList) {
            buffer.putInt(entryBytes.length);
            buffer.put(entryBytes);
        }

        return buffer.array();
    }

    public static AppendEntry deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.APPEND_ENTRY)) {
                throw new IllegalArgumentException("Invalid message errorType " + messageType);
            }

            long id = buffer.getLong();
            long term = buffer.getLong();

            int allEntryBytesLength = buffer.getInt();
            byte[] allEntryBytes = new byte[allEntryBytesLength];
            buffer.get(allEntryBytes);
            ByteBuffer allEntryBuffer = ByteBuffer.wrap(allEntryBytes);

            List<RaftLogHandler.Log> entries = new ArrayList<>();
            while (allEntryBuffer.hasRemaining()) {
                int entryLength = allEntryBuffer.getInt();
                byte[] entryBytes = new byte[entryLength];
                allEntryBuffer.get(entryBytes);
                RaftLogHandler.Log entry = RaftLogHandler.Log.deserialize(entryBytes);
                entries.add(entry);
            }

            return new AppendEntry(id, term, entries);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "AppendEntry{" +
                "errorType=" + type +
                ", id=" + id +
                ", term=" + term +
                ", entries=" + entries +
                '}';
    }
}
