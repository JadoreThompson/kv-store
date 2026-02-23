package com.zenz.kvstore;

public enum MessageType {
    PING_REQUEST(0),
    PING_RESPONSE(1),
    LOG_REQUEST(2),
    LOG_RESPONSE(3);

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