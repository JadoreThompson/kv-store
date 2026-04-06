package com.zenz.kvstore.common.responses;

import com.zenz.kvstore.common.enums.ResponseType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record GetResponse(ResponseType type, byte[] value) implements BaseResponse {

    public GetResponse(byte[] value) {
        this(ResponseType.GET_RESPONSE, value);
    }

    public boolean isNull() {
        return value == null;
    }

    @Override
    public byte[] serialize() {
        if (value == null) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
            buffer.putInt(type.getValue());
            buffer.putInt(-1);  // -1 indicates null
            return buffer.array();
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + value.length);

        buffer.putInt(type.getValue());
        buffer.putInt(value.length);
        buffer.put(value);

        return buffer.array();
    }

    public static GetResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            ResponseType type = ResponseType.fromValue(typeValue);

            if (!type.equals(ResponseType.GET_RESPONSE)) {
                throw new IllegalArgumentException("Invalid type code received " + type);
            }

            int valueLength = buffer.getInt();

            // Handle null case: -1 indicates null
            if (valueLength == -1) {
                return new GetResponse(null);
            }

            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            return new GetResponse(valueBytes);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
