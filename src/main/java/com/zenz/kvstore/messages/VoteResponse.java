package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;

import java.nio.ByteBuffer;

/**
 * Response to a VoteRequest in the Raft leader election process.
 * Contains the responder's current term and whether the vote was granted.
 */
public record VoteResponse(MessageType type, long term, boolean voteGranted) implements Message {

    /**
     * Creates a VoteResponse with the specified term and vote decision.
     *
     * @param term         the responder's current term
     * @param voteGranted  whether the vote was granted to the candidate
     */
    public VoteResponse(long term, boolean voteGranted) {
        this(MessageType.VOTE_RESPONSE, term, voteGranted);
    }

    /**
     * Serializes the VoteResponse to a byte array.
     * Format: [type(4)] [term(8)] [voteGranted(1)]
     *
     * @return the serialized byte array
     */
    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1);

        buffer.putInt(type().getValue());
        buffer.putLong(term);
        buffer.put((byte) (voteGranted ? 1 : 0));

        return buffer.array();
    }

    /**
     * Deserializes a VoteResponse from a ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the serialized VoteResponse
     * @return the deserialized VoteResponse
     * @throws IllegalArgumentException if the message type is not VOTE_RESPONSE
     */
    public static VoteResponse deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        MessageType type = MessageType.fromValue(typeValue);
        if (!type.equals(MessageType.VOTE_RESPONSE))
            throw new IllegalArgumentException("Invalid message type " + type.name());

        long term = buffer.getLong();
        boolean voteGranted = buffer.get() == 1;

        return new VoteResponse(term, voteGranted);
    }

    @Override
    public String toString() {
        return "VoteResponse{term=" + term + ", voteGranted=" + voteGranted + "}";
    }
}