package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;


public class PingResponse extends Message {
    @Override
    public MessageType type() {
        return MessageType.PING_RESPONSE;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(type().getValue());
        return buffer.array();
    }

    public static PingResponse deserialize(ByteBuffer buffer) {
        // Type is already read by the caller
        return new PingResponse();
    }

    public static PingResponse fromString(String[] components) {
        if (components.length != 1) throw new IllegalArgumentException("PingResponse must only have one component");
        return new PingResponse();
    }

    @Override
    public String toString() {
        return type().toString() + " PONG";
    }
}
