package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record AppendSnapshotV2(
        MessageType type,
        byte[] snapshot,
        long logId,
        long term
) implements BaseMessage {

    public AppendSnapshotV2(byte[] snapshot, long logId, long term) {
        this(MessageType.APPEND_SNAPSHOT, snapshot, logId, term);
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + snapshot.length);

        buffer.putInt(type.getValue());
        buffer.putLong(logId);
        buffer.putLong(term);
        buffer.putInt(snapshot.length);
        buffer.put(snapshot);

        return buffer.array();
    }

    public static AppendSnapshotV2 deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.APPEND_SNAPSHOT)) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            long logId = buffer.getLong();
            long term = buffer.getLong();
            int snapshotLength = buffer.getInt();
            byte[] snapshot = new byte[snapshotLength];
            buffer.get(snapshot);

            return new AppendSnapshotV2(snapshot, logId, term);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "AppendSnapshotV2{" +
                "type=" + type +
                ", snapshot=" + (snapshot != null ? snapshot.length + " bytes" : "null") +
                ", logId=" + logId +
                ", term=" + term +
                '}';
    }
}

