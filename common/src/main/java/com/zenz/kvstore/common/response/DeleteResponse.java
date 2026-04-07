package com.zenz.kvstore.common.response;

import com.zenz.kvstore.common.enums.ResponseType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record DeleteResponse(ResponseType type, boolean success) implements BaseResponse {

    public DeleteResponse(boolean success) {
        this(ResponseType.DELETE_RESPONSE, success);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1);
        buffer.putInt(type.getValue());
        buffer.put((byte) (success ? 1 : 0));

        return buffer.array();
    }

    public static DeleteResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            ResponseType type = ResponseType.fromValue(typeValue);
            if (!type.equals(ResponseType.DELETE_RESPONSE)) {
                throw new IllegalArgumentException("Invalid response type " + type);
            }

            boolean success = buffer.get() == 1;
            return new DeleteResponse(success);
        } catch (BufferUnderflowException e) {
            System.err.println("Error deserializing DeleteResponse " + e.getMessage());
            return null;
        }
    }
}
