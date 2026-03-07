package com.zenz.kvstore;

public enum MessageType {
    APPEND_ENTRY(1),
    INSTALL_SNAPSHOT(2),
    REQUEST_ENTRY(3),
    ERROR(4),
    REQUEST_VOTE(5),
    LEADER_ELECTED(6),
    APPEND_ENTRY_RESPONSE(7),
    HEARTBEAT_REQUEST(8),
    HEARTBEAT_RESPONSE(9),
    REQUEST_VOTE_RESPONSE(10),
    REDIRECT(11);

    private final int value;

    private MessageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static MessageType fromValue(int value) {
        for (MessageType type : MessageType.values()) {
            if (type.value == value) return type;
        }

        throw new IllegalArgumentException("Invalid value " + value);
    }

    @Override
    public String toString() {
        return name();
    }
}
