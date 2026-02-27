package com.zenz.kvstore.responses;

import com.zenz.kvstore.ResponseStatus;
import com.zenz.kvstore.ResponseType;

import java.nio.ByteBuffer;

public interface BaseResponse {
    ResponseStatus status();

    default ResponseType type() {
        throw new UnsupportedOperationException();
    }

    byte[] serialize();

    static BaseResponse deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int statusValue = buffer.getInt();
        ResponseStatus status = ResponseStatus.fromValue(statusValue);
        if (status.equals(ResponseStatus.ERROR)) {
            return ErrorResponse.deserialize(bytes);
        }

        int typeValue = buffer.getInt();
        ResponseType type = ResponseType.fromValue(typeValue);

        if (type.equals(ResponseType.LOG)) return LogResponse.deserialize(bytes);
        if (type.equals(ResponseType.HEARTBEAT)) return HeartbeatResponse.deserialize(bytes);

        throw new IllegalArgumentException("Unknown request type");
    }
}
