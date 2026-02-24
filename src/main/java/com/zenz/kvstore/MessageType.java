package com.zenz.kvstore;

public enum MessageType {
    PING_REQUEST(1),
    PING_RESPONSE(2),
    LOG_REQUEST(3),
    LOG_RESPONSE(4),
    BROKER_LOG_STATE_REQUEST(5);
    private final int value;

    private MessageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static MessageType fromValue(int value) {
        for (MessageType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown message type value: " + value);
    }

    @Override
    public String toString() {
        return "" + value;
    }
}