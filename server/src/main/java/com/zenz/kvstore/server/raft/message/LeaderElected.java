package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record LeaderElected(MessageType type, long term, String name) implements Message {

    public LeaderElected(long term, String name) {
        this(MessageType.LEADER_ELECTED, term, name);
    }

    public static LeaderElected deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.LEADER_ELECTED)) {
                throw new IllegalArgumentException("Invalid message errorType " + messageType);
            }

            long term = buffer.getLong();
            int nameLength = buffer.getInt();
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);

            return new LeaderElected(messageType, term, name);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public byte[] serialize() {
        byte[] nameBytes = name().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(
                4 // type
                        + 8 // term
                        + 4 // name bytes length
                        + nameBytes.length // name bytes
        );

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);

        return buffer.array();
    }

    @Override
    public String toString() {
        return "LeaderElected{" +
                "type=" + type +
                ", term=" + term +
                ", name=" + name +
                '}';
    }
}
