package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

public abstract class Message {
    public abstract MessageType type();

    public static Message fromString(String value) {
        String[] components = value.strip().split(" ");
        MessageType type = MessageType.valueOf(components[0]);

        if (type.equals(MessageType.PING_REQUEST)) return PingRequest.fromString(components);
        if (type.equals(MessageType.PING_RESPONSE)) return PingResponse.fromString(components);

        throw new IllegalArgumentException("Unknown message type: " + value);
    }

    public static Message fromString(String[] components) {
        throw new UnsupportedOperationException();
    }

    public byte[] serialize() {
        throw new UnsupportedOperationException();
    }

    public static Message deserialize(ByteBuffer value) {
        int type = value.getInt();
        MessageType messageType = MessageType.fromValue(type);

        if (messageType.equals(MessageType.PING_REQUEST)) return PingRequest.deserialize(value);
        if (messageType.equals(MessageType.PING_RESPONSE)) return PingResponse.deserialize(value);

        throw new IllegalArgumentException("Unknown message type: " + value);
    }

    @Override
    public abstract String toString();
}
