package com.zenz.kvstore;

public enum CommandType {
    GET("GET"),
    PUT("PUT");

    private final String value;

    private CommandType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}

