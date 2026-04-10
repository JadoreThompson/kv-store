package com.zenz.kvstore.server.raft.message;

import java.nio.ByteBuffer;

public interface Message {

    RaftMessageType type();

    static Message deserialize(final ByteBuffer buffer) {
        final RaftMessageType messageType = RaftMessageType.fromValue(buffer.getInt());
        buffer.rewind();

        return switch (messageType) {
            case APPEND_ENTRY -> AppendEntry.deserialize(buffer);
            case INSTALL_SNAPSHOT -> InstallSnapshot.deserialize(buffer);
            case REQUEST_VOTE -> RequestVote.deserialize(buffer);
            case REQUEST_VOTE_RESPONSE -> RequestVoteResponse.deserialize(buffer);
            default -> throw new IllegalArgumentException("Unknown message type: " + messageType);
        };
    }
}
