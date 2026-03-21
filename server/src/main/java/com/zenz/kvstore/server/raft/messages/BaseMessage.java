package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.ByteBuffer;

public interface BaseMessage {
    MessageType type();

    byte[] serialize();

    static BaseMessage deserialize(ByteBuffer buffer) {
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
        if (type.equals(MessageType.REDIRECT)) {
            return RedirectMessage.deserialize(buffer);
        }
        if (type.equals(MessageType.REGISTER)) {
            return RegisterMessage.deserialize(buffer);
        }
        if (type.equals(MessageType.ERROR)) {
            return ErrorMessage.deserialize(buffer);
        }

        throw new IllegalArgumentException("Unknown message errorType: " + type);
    }
}
