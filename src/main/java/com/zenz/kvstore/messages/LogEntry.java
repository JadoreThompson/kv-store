package com.zenz.kvstore.messages;

import com.zenz.kvstore.operations.Operation;

public record LogEntry(int term, int index, Operation operation, boolean committed) {
    public String toString() {
        return term + " " + index + " " + operation + " " + committed;
    }
}
