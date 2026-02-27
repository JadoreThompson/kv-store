package com.zenz.kvstore.responses;

import com.zenz.kvstore.ResponseStatus;
import com.zenz.kvstore.ResponseType;

import java.nio.ByteBuffer;

public record HeartbeatResponse(ResponseStatus status, ResponseType type) implements BaseResponse {
    public HeartbeatResponse() {
        this(ResponseStatus.SUCCESS, ResponseType.HEARTBEAT);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
        buffer.putInt(status.getValue());
        buffer.putInt(type.getValue());
        return buffer.array();
    }

    static HeartbeatResponse deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int statusValue = buffer.getInt();

        int typeValue = buffer.getInt();
        ResponseType type = ResponseType.fromValue(typeValue);
        if (!type.equals(ResponseType.HEARTBEAT)) {
            throw new IllegalArgumentException("Invalid message type");
        }

        return new HeartbeatResponse();
    }
}
