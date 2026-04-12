package com.zenz.kvstore.server.raft.message;

import lombok.Getter;

import java.nio.ByteBuffer;

public record AppendEntryResponse(
        RaftMessageType type,
        long term,
        FailureReason failureReason,
        long prevLogId,
        long prevLogTerm,
        long lastLogId,
        long lastLogTerm) implements Message {

    public AppendEntryResponse(
            long term,
            FailureReason reason,
            long prevLogId,
            long prevLogTerm,
            long lastLogId,
            long lastLogTerm) {
        this(RaftMessageType.APPEND_ENTRY_RESPONSE, term, reason, prevLogId, prevLogTerm, lastLogId, lastLogTerm);
    }

    public boolean isSuccess() {
        return failureReason == null;
    }

    @Override
    public byte[] serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(
                4 + // type
                        8 + // term
                        4 + // failureReason
                        8 + // prevLogId
                        8   // prevLogTerm
        );

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putInt(failureReason != null ? failureReason.getValue() : 0);
        buffer.putLong(prevLogId);
        buffer.putLong(prevLogTerm);
        buffer.putLong(lastLogId);
        buffer.putLong(lastLogTerm);

        return buffer.array();
    }

    public static AppendEntryResponse deserialize(final ByteBuffer buffer) {
        final RaftMessageType type = RaftMessageType.fromValue(buffer.getInt());
        final long term = buffer.getLong();

        final int reasonValue = buffer.getInt();
        final FailureReason reason =
                reasonValue == 0 ? null : FailureReason.fromValue(reasonValue);

        final long prevLogId = buffer.getLong();
        final long prevLogTerm = buffer.getLong();
        final long lastLogId = buffer.getLong();
        final long lastLogTerm = buffer.getLong();

        return new AppendEntryResponse(
                type,
                term,
                reason,
                prevLogId,
                prevLogTerm,
                lastLogId,
                lastLogTerm
        );
    }

    public enum FailureReason {
        /**
         * Local term is greater than the term within the {@link AppendEntry} message
         */
        TERM(1),

        /**
         * Previous logs doesn't contain a log entry with prevLogId and prevLogTerm. The local
         * prevLogId and prevLogTerm will be provided in the response for the leader to failureReason with.
         */
        PREV_LOG(2);

        @Getter
        private final int value;

        FailureReason(final int value) {
            this.value = value;
        }

        public static FailureReason fromValue(final int value) {
            for (FailureReason reason : FailureReason.values()) {
                if (reason.value == value) {
                    return reason;
                }
            }

            throw new IllegalArgumentException("Invalid FailureReason value: " + value);
        }
    }
}
