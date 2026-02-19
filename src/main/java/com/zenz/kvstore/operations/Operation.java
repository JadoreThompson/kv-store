package com.zenz.kvstore.operations;

import com.zenz.kvstore.OperationType;

public interface Operation {
    int id();

    OperationType type();

    static Operation fromLine(String line) {
        String[] components = line.strip().split(" ");
        int id = Integer.parseInt(components[0]);
        OperationType type = OperationType.valueOf(components[1]);

        return switch (type) {
            case PUT -> PutOperation.fromLine(id, components);
            case GET -> GetOperation.fromLine(id, components);
        };
    }
}