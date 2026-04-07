package com.zenz.kvstore.common.enums;

public enum CommandType {

    GET(1),
    PUT(2),
    DELETE(3),
    SEARCH(4);

    private final int value;

    CommandType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static CommandType fromValue(int value) {
        for (CommandType type : CommandType.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Unknown CommandType value: " + value);
    }
}

