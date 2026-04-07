package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.ByteBuffer;

public interface Message {

    MessageType type();

    byte[] serialize();

    static Message deserialize(ByteBuffer buffer) {
        final int typeValue = buffer.getInt();
        final MessageType type = MessageType.fromValue(typeValue);
        buffer.rewind();

        return switch (type) {
            case INSTALL_SNAPSHOT -> InstallSnapshot.deserialize(buffer);
            case APPEND_ENTRY -> AppendEntry.deserialize(buffer);
            case REQUEST_ENTRY -> RequestEntry.deserialize(buffer);
            case REQUEST_VOTE -> RequestVote.deserialize(buffer);
            case REQUEST_VOTE_RESPONSE -> RequestVoteResponse.deserialize(buffer);
            case LEADER_ELECTED -> LeaderElected.deserialize(buffer);
            case APPEND_ENTRY_RESPONSE -> AppendEntryResponse.deserialize(buffer);
            case HEARTBEAT_REQUEST -> HeartbeatRequest.deserialize(buffer);
            case HEARTBEAT_RESPONSE -> HeartbeatResponse.deserialize(buffer);
            case REDIRECT -> RedirectMessage.deserialize(buffer);
            case REGISTER -> RegisterMessage.deserialize(buffer);
            case ERROR -> ErrorMessage.deserialize(buffer);
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        };
    }
}
