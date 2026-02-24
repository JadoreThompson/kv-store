package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

public class PingRequest extends Message {

    public PingRequest() {
    }

    @Override
    public MessageType type() {
        return MessageType.PING_REQUEST;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(type().getValue());
        return buffer.array();
    }

    public static PingRequest deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int type = buffer.getInt();
        MessageType messageType = MessageType.fromValue(type);
        if (!messageType.equals(MessageType.PING_REQUEST))
            throw new IllegalArgumentException(
                    "Invalid type "
                            + type
                            + " for ping request. Expected "
                            + MessageType.PING_RESPONSE.getValue());

        return new PingRequest();
    }

    @Override
    public String toString() {
        return type().toString();
    }

    public static Message fromString(String[] components) {
        if (components.length != 1) throw new IllegalArgumentException("PingRequest must only have one component");
        return new PingRequest();
    }
}