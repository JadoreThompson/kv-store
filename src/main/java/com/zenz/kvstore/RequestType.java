package com.zenz.kvstore;

public enum RequestType {
    LOG(1),
    HEARTBEAT(2),
    BROADCAST(3);

    private final int value;

    RequestType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static RequestType fromValue(int value) {
        for (RequestType type : RequestType.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Invalid request type " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
