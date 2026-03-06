package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;

import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record RedirectMessage(
        MessageType type,
        InetSocketAddress address
) implements BaseMessage {

    public RedirectMessage(InetSocketAddress address) {
        this(MessageType.REDIRECT, address);
    }

    @Override
    public byte[] serialize() {
        String host = address.getHostString();
        int port = address.getPort();
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + hostBytes.length + 4);

        buffer.putInt(type.getValue());
        buffer.putInt(hostBytes.length);
        buffer.put(hostBytes);
        buffer.putInt(port);

        return buffer.array();
    }

    public static RedirectMessage deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.REDIRECT)) {
                throw new IllegalArgumentException("Invalid message errorType " + messageType);
            }

            int hostLength = buffer.getInt();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);

            int port = buffer.getInt();

            InetSocketAddress address = new InetSocketAddress(host, port);

            return new RedirectMessage(messageType, address);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "RedirectMessage{" +
                "errorType=" + type +
                ", nodeAddress=" + address +
                '}';
    }
}