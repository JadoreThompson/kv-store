package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

public record HeartbeatRequest(MessageType type) implements Message {
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
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int typeValue = buffer.getInt();
        MessageType type = MessageType.fromValue(typeValue);
        if (!type.equals(MessageType.HEARTBEAT_REQUEST)) {
            throw new IllegalArgumentException("Invalid message type");
        }
        return new HeartbeatRequest();
    }
}
