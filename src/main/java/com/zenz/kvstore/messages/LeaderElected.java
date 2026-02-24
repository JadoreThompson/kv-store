package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

/**
 * Message broadcast when a new leader is elected in the Raft cluster.
 * Contains the leader's logId, term, and brokerId.
 */
public record LeaderElected(MessageType type, long logId, long term, long brokerId) implements Message {

    /**
     * Creates a LeaderElected message with the specified logId, term, and brokerId.
     *
     * @param logId    the leader's last log entry ID
     * @param term     the leader's current term
     * @param brokerId the broker ID of the elected leader
     */
    public LeaderElected(long logId, long term, long brokerId) {
        this(MessageType.LEADER_ELECTED, logId, term, brokerId);
    }

    /**
     * Serializes the LeaderElected message to a byte array.
     * Format: [type(4)] [logId(8)] [term(8)] [brokerId(8)]
     *
     * @return the serialized byte array
     */
    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 8);

        buffer.putInt(type().getValue());
        buffer.putLong(logId);
        buffer.putLong(term);
        buffer.putLong(brokerId);

        return buffer.array();
    }

    /**
     * Deserializes a LeaderElected message from a ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the serialized LeaderElected message
     * @return the deserialized LeaderElected message
     * @throws IllegalArgumentException if the message type is not LEADER_ELECTED
     */
    public static LeaderElected deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        MessageType type = MessageType.fromValue(typeValue);
        if (!type.equals(MessageType.LEADER_ELECTED))
            throw new IllegalArgumentException("Invalid message type " + type.name());

        long logId = buffer.getLong();
        long term = buffer.getLong();
        long brokerId = buffer.getLong();

        return new LeaderElected(logId, term, brokerId);
    }

    @Override
    public String toString() {
        return "LeaderElected{logId=" + logId + ", term=" + term + ", brokerId=" + brokerId + "}";
    }
}