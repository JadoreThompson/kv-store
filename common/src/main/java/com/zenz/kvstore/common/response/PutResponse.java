package com.zenz.kvstore.common.response;

import com.zenz.kvstore.common.enums.ResponseType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record PutResponse(ResponseType type) implements BaseResponse {

    public PutResponse() {
        this(ResponseType.PUT_RESPONSE);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(type.getValue());
        return buffer.array();
    }

    public static PutResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            ResponseType type = ResponseType.fromValue(typeValue);

            if (!type.equals(ResponseType.PUT_RESPONSE)) {
                throw new IllegalArgumentException("Invalid type value: " + type);
            }

            return new PutResponse();
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}