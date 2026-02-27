package com.zenz.kvstore;

public enum ResponseStatus {
    SUCCESS(1),
    ERROR(2);

    private int value;

    ResponseStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ResponseStatus fromValue(int value) {
        for (ResponseStatus status : ResponseStatus.values()) {
            if (status.getValue() == value) return status;
        }

        throw new IllegalArgumentException("Invalid value " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
