package com.zenz.kvstore;

public enum MessageType {
    LOG_REQUEST(1),
    LOG_RESPONSE(2),
    LOG_BROADCAST(3);

    private final int value;

    private MessageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static MessageType fromValue(int value) {
        for (MessageType type : MessageType.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Unknown message type: " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
