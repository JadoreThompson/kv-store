package com.zenz.kvstore.responses;

import com.zenz.kvstore.ResponseType;

import java.nio.ByteBuffer;

public interface BaseResponse {
    ResponseType type();

    byte[] serialize();

    public static BaseResponse deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        ResponseType type = ResponseType.fromValue(typeValue);

        buffer.rewind();
        if (type.equals(ResponseType.PUT_RESPONSE)) {
            return PutResponse.deserialize(buffer);
        }
        if (type.equals(ResponseType.GET_RESPONSE)) {
            return GetResponse.deserialize(buffer);
        }
        if (type.equals(ResponseType.REDIRECT_RESPONSE)) {
            return RedirectResponse.deserialize(buffer);
        }
        if (type.equals(ResponseType.ERROR_RESPONSE)) {
            return ErrorResponse.deserialize(buffer);
        }

        throw new IllegalArgumentException("Unknown response type: " + type);
    }
}
