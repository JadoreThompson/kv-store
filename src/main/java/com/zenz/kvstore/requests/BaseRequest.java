package com.zenz.kvstore.requests;

import com.zenz.kvstore.RequestType;

import java.nio.ByteBuffer;

public interface BaseRequest {
    byte[] serialize();

    RequestType type();

    static BaseRequest deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int typeValue = buffer.getInt();
        RequestType type = RequestType.fromValue(typeValue);

        if (type.equals(RequestType.LOG)) return LogRequest.deserialize(bytes);
        if (type.equals(RequestType.HEARTBEAT)) return HeartbeatRequest.deserialize(bytes);
        if (type.equals(RequestType.BROADCAST)) return LogBroadcastRequest.deserialize(bytes);

        throw new IllegalArgumentException("Unknown request type");
    }
}
