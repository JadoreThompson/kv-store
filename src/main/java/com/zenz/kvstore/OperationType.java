package com.zenz.kvstore;

public enum OperationType {
    GET("GET"),
    PUT("PUT");

    private final String value;

    private OperationType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
