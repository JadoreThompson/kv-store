package com.zenz.kvstore;

public enum OperationType {
    GET(0),
    PUT(1);

    private final int value;

    private OperationType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static OperationType fromValue(int value) {
        for (OperationType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown operation type value: " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}

