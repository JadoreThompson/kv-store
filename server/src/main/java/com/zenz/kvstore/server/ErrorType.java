package com.zenz.kvstore.server;

public enum ErrorType {
    IN_ELECTION(1),
    UNSUPPORTED_OPERATION(2),
    SERVER_ERROR(3);

    private final int value;

    private ErrorType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ErrorType fromValue(int value) {
        for (ErrorType type : ErrorType.values()) {
            if (type.getValue() == value) return type;
        }

        throw new IllegalArgumentException("Invalid type value " + value);
    }
}