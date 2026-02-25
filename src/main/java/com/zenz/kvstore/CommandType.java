package com.zenz.kvstore;

public enum CommandType {
    GET(1),
    PUT(2);

    private final int value;

    private CommandType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static CommandType fromValue(int value) {
        for (CommandType type : CommandType.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Unknown command type: " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}

