package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.operations.RaftOperation;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;

public record ControllerLogStateResponse(
        MessageType type,
        Type documentType,
        byte[] snapshot,
        RaftOperation operation
) implements Message {
    public ControllerLogStateResponse(Type documentType, byte[] snapshot, RaftOperation operation) {
        this(MessageType.CONTROLLER_LOG_STATE_RESPONSE, documentType, snapshot, operation);
    }

    @Override
    public MessageType type() {
        return MessageType.CONTROLLER_LOG_STATE_RESPONSE;
    }

    public byte[] serialize() {
        byte[] dataBytes;

        if (snapshot != null) {
            dataBytes = snapshot;
        } else {
            dataBytes = operation.serialize();
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + dataBytes.length);

        buffer.putInt(type().getValue());
        buffer.putInt(documentType.getValue());
        buffer.putInt(dataBytes.length);
        buffer.put(dataBytes);

        return buffer.array();
    }

    public static ControllerLogStateResponse deserialize(ByteBuffer buffer) {
        int type = buffer.getInt();
        MessageType messageType = MessageType.fromValue(type);
        if (!messageType.equals(MessageType.CONTROLLER_LOG_STATE_RESPONSE))
            throw new IllegalArgumentException("Invalid message type " + messageType.name());

        int documentTypeValue = buffer.getInt();
        Type documentType = Type.fromValue(documentTypeValue);
        int dataByteLength = buffer.getInt();
        byte[] dataBytes = new byte[dataByteLength];
        buffer.get(dataBytes);

        return new ControllerLogStateResponse(
                documentType,
                documentType.equals(Type.SNAPSHOT)
                        ? dataBytes : null,
                documentType.equals(Type.LOG)
                        ? RaftOperation.deserialize(ByteBuffer.wrap(dataBytes)) : null
        );
    }

    public enum Type {
        LOG(1),
        SNAPSHOT(2);

        private final int value;

        private Type(int value) {
            this.value = value;
        }

        public static Type fromValue(int value) {
            for (Type type : Type.values()) {
                if (type.value == value) {
                    return type;
                }
            }

            throw new InvalidParameterException("Invalid value " + value);
        }

        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return name();
        }
    }
}
