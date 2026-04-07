package com.zenz.kvstore.server.raft;

public enum NodeRole {

    BROKER(1),
    CANDIDATE(2),
    CONTROLLER(3);

    private final int value;

    NodeRole(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static NodeRole fromValue(int value) {
        for (NodeRole type : NodeRole.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Invalid errorType " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
