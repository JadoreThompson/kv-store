package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

public record GetOperation(int id, String key) implements Operation {

    @Override
    public OperationType type() {
        return OperationType.GET;
    }

    public static GetOperation fromLine(int id, String[] components) {
        String key = components[2];
        return new GetOperation(id, key);
    }
}