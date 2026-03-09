package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record HeartbeatRequest(MessageType type) implements BaseMessage {
    public HeartbeatRequest() {
        this(MessageType.HEARTBEAT_REQUEST);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(type.getValue());
        return buffer.array();
    }

    static HeartbeatRequest deserialize(byte[] bytes) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int typeValue = buffer.getInt();
            MessageType type = MessageType.fromValue(typeValue);
            if (!type.equals(MessageType.HEARTBEAT_REQUEST)) {
                throw new IllegalArgumentException("Invalid message errorType");
            }
            return new HeartbeatRequest();
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    static HeartbeatRequest deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType type = MessageType.fromValue(typeValue);
            if (!type.equals(MessageType.HEARTBEAT_REQUEST)) {
                throw new IllegalArgumentException("Invalid message errorType");
            }
            return new HeartbeatRequest();
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}