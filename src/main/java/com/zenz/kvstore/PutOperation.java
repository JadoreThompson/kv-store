package com.zenz.kvstore;

import java.nio.charset.StandardCharsets;

public record PutOperation(int id, String key, byte[] value) implements Operation {
    
    @Override
    public OperationType type() {
        return OperationType.PUT;
    }
    
    public static PutOperation fromLine(int id, String[] components) {
        String key = components[2];
        String valueStr = components[3];
        return new PutOperation(id, key, valueStr.getBytes(StandardCharsets.UTF_8));
    }
}