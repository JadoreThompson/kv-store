package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

public interface BaseMessage {
    MessageType type();

    byte[] serialize();

    public static BaseMessage deserialize(ByteBuffer buffer) {
        final String debugPrefix = "[BaseMessage][deserialize] ";
        int typeValue = buffer.getInt();
        MessageType type = MessageType.fromValue(typeValue);

        buffer.rewind();
        if (type.equals(MessageType.INSTALL_SNAPSHOT)) {
            return InstallSnapshot.deserialize(buffer);
        }
        if (type.equals(MessageType.APPEND_ENTRY)) {
            return AppendEntry.deserialize(buffer);
        }
        if (type.equals(MessageType.REQUEST_ENTRY)) {
            return RequestEntry.deserialize(buffer);
        }
        if (type.equals(MessageType.REQUEST_VOTE)) {
            return RequestVote.deserialize(buffer);
        }
        if (type.equals(MessageType.REQUEST_VOTE_RESPONSE)) {
            return RequestVoteResponse.deserialize(buffer);
        }
        if (type.equals(MessageType.LEADER_ELECTED)) {
            return LeaderElected.deserialize(buffer);
        }
        if (type.equals(MessageType.APPEND_ENTRY_RESPONSE)) {
            return AppendEntryResponse.deserialize(buffer);
        }
        if (type.equals(MessageType.HEARTBEAT_REQUEST)) {
            return HeartbeatRequest.deserialize(buffer);
        }
        if (type.equals(MessageType.HEARTBEAT_RESPONSE)) {
            return HeartbeatResponse.deserialize(buffer);
        }
        if (type.equals(MessageType.ERROR)) {
            return ErrorMessage.deserialize(buffer);
        }

        throw new IllegalArgumentException("Unknown message type: " + type);
    }
}
