package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

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

    ;

    @Override
    public abstract String toString();
}
