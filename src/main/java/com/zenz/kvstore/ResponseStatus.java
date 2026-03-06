package com.zenz.kvstore;

public enum ResponseStatus {
    SUCCESS(1),
    ERROR(2);

    private final int value;

    private ResponseStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ResponseStatus fromValue(int value) {
        for (ResponseStatus type : ResponseStatus.values()) {
            if (type.getValue() == value) return type;
        }

        throw new IllegalArgumentException("Invalid errorType value " + value);
    }
}
