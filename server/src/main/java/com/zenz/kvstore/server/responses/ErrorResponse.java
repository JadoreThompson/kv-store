package com.zenz.kvstore.server.responses;

import com.zenz.kvstore.server.ErrorType;
import com.zenz.kvstore.server.ResponseType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record ErrorResponse(ResponseType type, ErrorType errorType, String message) implements BaseResponse {

    public ErrorResponse(ErrorType errorType, String message) {
        this(ResponseType.ERROR_RESPONSE, errorType, message);
    }

    @Override
    public byte[] serialize() {
        byte[] messageBytes = message != null ? message.getBytes(StandardCharsets.UTF_8) : new byte[0];

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + messageBytes.length);

        buffer.putInt(type.getValue());
        buffer.putInt(errorType.getValue());
        buffer.putInt(messageBytes.length);
        buffer.put(messageBytes);

        return buffer.array();
    }

    public static ErrorResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            ResponseType type = ResponseType.fromValue(typeValue);

            if (!type.equals(ResponseType.ERROR_RESPONSE)) {
                throw new IllegalArgumentException("Invalid response type " + type);
            }

            int errorTypeValue = buffer.getInt();
            ErrorType errorType = ErrorType.fromValue(errorTypeValue);

            int messageLength = buffer.getInt();
            byte[] messageBytes = new byte[messageLength];
            buffer.get(messageBytes);

            return new ErrorResponse(errorType, new String(messageBytes, StandardCharsets.UTF_8));
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}