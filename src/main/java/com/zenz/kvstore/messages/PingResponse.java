package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;


public class PingResponse extends Message {
    @Override
    public MessageType type() {
        return MessageType.PING_RESPONSE;
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