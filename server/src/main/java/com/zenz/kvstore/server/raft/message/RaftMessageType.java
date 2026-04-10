package com.zenz.kvstore.server.raft.message;

import lombok.Getter;

@Getter
public enum RaftMessageType {

    APPEND_ENTRY(1),
    INSTALL_SNAPSHOT(2),
    REQUEST_VOTE(3),
    REQUEST_VOTE_RESPONSE(4),
    LEADER_ELECTED(5);

    private final int value;

    RaftMessageType(final int value) {
        this.value = value;
    }

    public static RaftMessageType fromValue(final int value) {
        for (RaftMessageType type : RaftMessageType.values()) {
            if (type.value == value) {
                return type;
            }
        }

        throw new IllegalArgumentException("Invalid RaftMessageType value: " + value);
    }
}
