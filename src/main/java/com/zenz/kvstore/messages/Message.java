package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

//public abstract class Message {
public interface Message {
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

    public abstract byte[] serialize();

    public static Message deserialize(ByteBuffer buffer) {
        int type = buffer.getInt();
        MessageType messageType = MessageType.fromValue(type);
        buffer.rewind();

        if (messageType.equals(MessageType.PING_REQUEST)) return PingRequest.deserialize(buffer);
        if (messageType.equals(MessageType.PING_RESPONSE)) return PingResponse.deserialize(buffer);
        if (messageType.equals(MessageType.BROKER_LOG_STATE_REQUEST)) return BrokerLogStateRequest.deserialize(buffer);

        throw new IllegalArgumentException("Unknown message type " + messageType.name());
    }

    @Override
    public abstract String toString();
}
