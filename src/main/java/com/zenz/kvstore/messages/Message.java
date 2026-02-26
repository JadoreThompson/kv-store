package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

public interface Message {
    MessageType type();

    byte[] serialize();

    static Message deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int typeValue = buffer.getInt();
        MessageType type = MessageType.fromValue(typeValue);

        if (type.equals(MessageType.LOG_REQUEST)) return LogRequest.deserialize(bytes);
        if (type.equals(MessageType.LOG_RESPONSE)) return LogResponse.deserialize(bytes);
        if (type.equals(MessageType.LOG_BROADCAST)) return LogBroadcast.deserialize(bytes);

        throw new IllegalArgumentException("Invalid message type " + type);
    }
}
