package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record AppendEntryV2Response(
        MessageType type,
        long term,
        boolean success,
        FailureCode code
) implements Message {

    public AppendEntryV2Response(long term, boolean success, FailureCode code) {
        this(MessageType.APPEND_ENTRY_RESPONSE, term, success, code);
        if (!success && code == null) {
            throw new IllegalArgumentException("A failure code must be provided if success is false");
        }
    }

    public static AppendEntryV2Response deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);

            if (messageType != MessageType.APPEND_ENTRY_RESPONSE) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            long term = buffer.getLong();
            boolean success = buffer.get() == 1;

            int codeValue = buffer.getInt();
            FailureCode code = codeValue == 0 ? null : FailureCode.fromValue(codeValue);

            return new AppendEntryV2Response(term, success, code);

        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(
                4 +
                        8 +
                        1 +
                        4);

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.put((byte) (success ? 1 : 0));
        buffer.putInt(code != null ? code.getValue() : 0);

        return buffer.array();
    }

    @Override
    public String toString() {
        return "AppendEntryV2Response{" +
                "type=" + type +
                ", term=" + term +
                ", success=" + success +
                ", code=" + code +
                '}';
    }

    public enum FailureCode {
        /**
         * The term stated within the append entry message is smaller than the current term of the recipient node
         */
        SMALLER_TERM(1),

        /**
         * The logs on the recipient node doesn't contain a log with prevLogId and prevTerm
         */
        PREV_MISMATCH(2);

        private final int value;

        FailureCode(final int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static FailureCode fromValue(final int value) {
            for (FailureCode c : FailureCode.values()) {
                if (c.value == value) {
                    return c;
                }
            }

            throw new IllegalArgumentException(("Invalid FailureCode value: " + value));
        }
    }
}
