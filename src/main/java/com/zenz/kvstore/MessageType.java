package com.zenz.kvstore;

public enum MessageType {
    PING_REQUEST("PING"),
    PING_RESPONSE("PONG"),
    LOG_REQUEST("LOG_REQUEST"),
    LOG_RESPONSE("LOG_RESPONSE");

    private final String value;

    private MessageType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}