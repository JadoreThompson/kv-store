package com.zenz.kvstore;

public enum RaftErrorType {
    INVALID_MESSAGE_TYPE(1),
    INVALID_TERM(2);

    private final int value;

    private RaftErrorType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static RaftErrorType fromValue(int value) {
        for (RaftErrorType type : RaftErrorType.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Invalid value " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
