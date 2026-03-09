package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record InstallSnapshot(
        MessageType type,
        byte[] snapshot,
        long logId,
        long term
) implements BaseMessage {

    public InstallSnapshot(byte[] snapshot, long logId, long term) {
        this(MessageType.INSTALL_SNAPSHOT, snapshot, logId, term);
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

    public static InstallSnapshot deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.INSTALL_SNAPSHOT)) {
                throw new IllegalArgumentException("Invalid message errorType " + messageType);
            }

            long logId = buffer.getLong();
            long term = buffer.getLong();
            int snapshotLength = buffer.getInt();
            byte[] snapshot = new byte[snapshotLength];
            buffer.get(snapshot);

            return new InstallSnapshot(snapshot, logId, term);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" +
                "errorType=" + type +
                ", snapshot=" + (snapshot != null ? snapshot.length + " bytes" : "null") +
                ", logId=" + logId +
                ", term=" + term +
                '}';
    }
}

