package com.zenz.kvstore.common.response;

import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.enums.ResponseType;

import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record RedirectResponse(
        ResponseType type,
        ErrorType errorType,
        InetSocketAddress address
) implements BaseErrorResponse {

    public RedirectResponse(InetSocketAddress address) {
        this(ResponseType.ERROR_RESPONSE, ErrorType.NOT_CONTROLLER, address);
    }

    @Override
    public byte[] serialize() {
        String host = address.getHostString();
        int port = address.getPort();
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + hostBytes.length + 4);

        buffer.putInt(type.getValue());
        buffer.putInt(errorType.getValue());
        buffer.putInt(hostBytes.length);
        buffer.put(hostBytes);
        buffer.putInt(port);

        return buffer.array();
    }

    public static RedirectResponse deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            ResponseType type = ResponseType.fromValue(typeValue);

            if (!type.equals(ResponseType.ERROR_RESPONSE)) {
                throw new IllegalArgumentException("Invalid response type " + type);
            }

            int errorTypeValue = buffer.getInt();
            ErrorType errorType = ErrorType.fromValue(errorTypeValue);
            if (!errorType.equals(ErrorType.NOT_CONTROLLER)) {
                throw new IllegalArgumentException("Invalid error type " + errorType);
            }

            int hostLength = buffer.getInt();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);

            String host = new String(hostBytes, StandardCharsets.UTF_8);
            int port = buffer.getInt();

            InetSocketAddress address = new InetSocketAddress(host, port);

            return new RedirectResponse(address);

        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}