package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.RaftErrorType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record ErrorMessage(
        MessageType type,
        RaftErrorType errorType,
        String message
) implements BaseMessage {

    public ErrorMessage(RaftErrorType errorType, String message) {
        this(MessageType.ERROR, errorType, message);
    }

    @Override
    public byte[] serialize() {
        byte[] messageBytes = message != null ? message.getBytes(StandardCharsets.UTF_8) : new byte[0];
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + messageBytes.length);

        buffer.putInt(type.getValue());
        buffer.putInt(errorType.getValue());
        buffer.putInt(messageBytes.length);
        buffer.put(messageBytes);

        return buffer.array();
    }

    public static ErrorMessage deserialize(ByteBuffer buffer) {
        try {
            MessageType messageType = MessageType.fromValue(buffer.getInt());
            if (!messageType.equals(MessageType.ERROR)) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            RaftErrorType errorType = RaftErrorType.fromValue(buffer.getInt());
            int messageLength = buffer.getInt();
            byte[] messageBytes = new byte[messageLength];
            buffer.get(messageBytes);

            return new ErrorMessage(errorType, messageBytes.length > 0 ? new String(messageBytes, StandardCharsets.UTF_8) : null);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
