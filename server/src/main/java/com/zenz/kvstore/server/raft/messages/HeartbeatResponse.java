package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record HeartbeatResponse(MessageType type) implements BaseMessage {
    public HeartbeatResponse() {
        this(MessageType.HEARTBEAT_RESPONSE);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(type.getValue());
        return buffer.array();
    }

    static HeartbeatResponse deserialize(byte[] bytes) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int typeValue = buffer.getInt();
            MessageType type = MessageType.fromValue(typeValue);
            if (!type.equals(MessageType.HEARTBEAT_RESPONSE)) {
                throw new IllegalArgumentException("Invalid message errorType " + type);
            }
            return new HeartbeatResponse();
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    public static HeartbeatResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType type = MessageType.fromValue(typeValue);
            if (!type.equals(MessageType.HEARTBEAT_RESPONSE)) {
                throw new IllegalArgumentException("Invalid message errorType");
            }
            return new HeartbeatResponse();
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}