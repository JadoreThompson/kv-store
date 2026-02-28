package com.zenz.kvstore.requests;

import com.zenz.kvstore.RequestType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record HeartbeatRequest(RequestType type) implements BaseRequest {
    public HeartbeatRequest() {
        this(RequestType.HEARTBEAT);
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
            RequestType type = RequestType.fromValue(typeValue);
            if (!type.equals(RequestType.HEARTBEAT)) {
                throw new IllegalArgumentException("Invalid message type");
            }
            return new HeartbeatRequest();
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    static HeartbeatRequest deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            RequestType type = RequestType.fromValue(typeValue);
            if (!type.equals(RequestType.HEARTBEAT)) {
                throw new IllegalArgumentException("Invalid message type");
            }
            return new HeartbeatRequest();
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
