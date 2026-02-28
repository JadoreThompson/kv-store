package com.zenz.kvstore;

public enum ResponseType {
    LOG(1),
    HEARTBEAT(2),
    BROADCAST(3);

    private final int value;

    private ResponseType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ResponseType fromValue(int value) {
        for (ResponseType type : ResponseType.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Invalid value " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
