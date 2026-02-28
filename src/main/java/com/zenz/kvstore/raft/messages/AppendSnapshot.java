package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record AppendSnapshot(MessageType type, byte[] snapshot) implements BaseMessage {

    public AppendSnapshot(byte[] snapshot) {
        this(MessageType.APPEND_SNAPSHOT, snapshot);
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + snapshot.length);

        buffer.putInt(type.getValue());
        buffer.putInt(snapshot.length);
        buffer.put(snapshot);

        return buffer.array();
    }

    public static AppendSnapshot deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.APPEND_SNAPSHOT)) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            int snapshotLength = buffer.getInt();
            byte[] snapshot = new byte[snapshotLength];
            buffer.get(snapshot);

            return new AppendSnapshot(snapshot);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
