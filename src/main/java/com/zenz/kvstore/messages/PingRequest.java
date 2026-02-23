package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

public class PingRequest extends Message {

    public PingRequest() {
    }

    @Override
    public MessageType type() {
        return MessageType.PING_REQUEST;
    }

    public static Message fromString(String[] components) {
        if (components.length != 1) throw new IllegalArgumentException("PingRequest must only have one component");
        return new PingRequest();
    }

    @Override
    public String toString() {
        return type().toString() + " PING";
    }
}