package com.zenz.kvstore.server.raft;

public enum NodeState {
    BROKER(1),
    CANDIDATE(2),
    CONTROLLER(3);

    private final int value;

    NodeState(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static NodeState fromValue(int value) {
        for (NodeState type : NodeState.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Invalid errorType " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
