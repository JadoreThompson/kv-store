package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.util.ByteArraySerializable;

import java.nio.ByteBuffer;

public interface Message extends ByteArraySerializable {

    RaftMessageType type();

    String toString();

    static Message deserialize(final ByteBuffer buffer) {
        final RaftMessageType messageType = RaftMessageType.fromValue(buffer.getInt());
        buffer.rewind();

        return switch (messageType) {
            case APPEND_ENTRY -> AppendEntry.deserialize(buffer);
            case INSTALL_SNAPSHOT -> InstallSnapshot.deserialize(buffer);
            case REQUEST_VOTE -> RequestVote.deserialize(buffer);
            case REQUEST_VOTE_RESPONSE -> RequestVoteResponse.deserialize(buffer);
            case APPEND_ENTRY_RESPONSE -> AppendEntryResponse.deserialize(buffer);
            case INSTALL_SNAPSHOT_RESPONSE -> InstallSnapshotResponse.deserialize(buffer);
            default -> throw new InvalidMessageException("Unknown message type: " + messageType);
        };
    }
}
