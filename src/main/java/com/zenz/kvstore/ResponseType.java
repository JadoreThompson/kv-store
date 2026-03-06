package com.zenz.kvstore;

public enum ResponseType {
    PUT_RESPONSE(1),
    GET_RESPONSE(2),
    REDIRECT_RESPONSE(3),
    ERROR_RESPONSE(4);

    private final int value;

    private ResponseType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ResponseType fromValue(int value) {
        for (ResponseType type : ResponseType.values()) {
            if (type.getValue() == value) return type;
        }

        throw new IllegalArgumentException("Invalid errorType value " + value);
    }
}
