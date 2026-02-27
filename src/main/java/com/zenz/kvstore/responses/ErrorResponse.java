package com.zenz.kvstore.responses;

import com.zenz.kvstore.ResponseStatus;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record ErrorResponse(ResponseStatus status, String message) implements BaseResponse {
    public ErrorResponse(String message) {
        this(ResponseStatus.ERROR, message);
    }

    @Override
    public byte[] serialize() {
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + messageBytes.length);

        buffer.putInt(status().getValue());
        buffer.putInt(messageBytes.length);
        buffer.put(messageBytes);

        return buffer.array();
    }

    static ErrorResponse deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int statusValue = buffer.getInt();
        ResponseStatus status = ResponseStatus.fromValue(statusValue);
        if (!status.equals(ResponseStatus.ERROR)) {
            throw new IllegalArgumentException("Invalid message status");
        }

        int messageLength = buffer.getInt();
        byte[] messageBytes = new byte[messageLength];
        buffer.get(messageBytes);
        String message = new String(messageBytes, StandardCharsets.UTF_8);

        return new ErrorResponse(message);
    }
}
