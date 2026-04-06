package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record RequestVote(
        MessageType type,
        String name,
        long term,
        long prevLogId,
        long prevTerm
) implements Message {

    public RequestVote(String name, long term, long prevLogId, long prevTerm) {
        this(MessageType.REQUEST_VOTE, name, term, prevLogId, prevTerm);
    }

    @Override
    public byte[] serialize() {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(
                4 // type
                        + 8  // term
                        + 4 // name bytes length
                        + nameBytes.length // name bytes
                        + 8
                        + 8
        );

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        buffer.putLong(prevLogId);
        buffer.putLong(prevTerm);

        return buffer.array();
    }

    public static RequestVote deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.REQUEST_VOTE)) {
                throw new IllegalArgumentException("Invalid message errorType " + messageType);
            }

            long term = buffer.getLong();
            int  nameLength = buffer.getInt();
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            long prevLogId = buffer.getLong();
            long prevTerm = buffer.getLong();

            return new RequestVote(
                    messageType,
                    new String(nameBytes, StandardCharsets.UTF_8),
                    term,
                    prevLogId,
                    prevTerm
            );
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "RequestVote{" +
                "type=" + type +
                ", term=" + term +
                ", name=" + name +
                ", prevLogId=" + prevLogId +
                ", prevTerm=" + prevTerm +
                '}';
    }
}