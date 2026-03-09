package com.zenz.kvstore.server.raft;

public enum RaftErrorType {
    INVALID_MESSAGE_TYPE(1),
    INVALID_TERM(2),
    LOG_NOT_FOUND(3),
    GREATER_TERM(4),
    GREATER_LOG_ID(5);

    private final int value;

    RaftErrorType(int value) {
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
