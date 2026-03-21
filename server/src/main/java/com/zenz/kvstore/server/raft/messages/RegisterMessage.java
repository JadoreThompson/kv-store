package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.raft.MessageType;

import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Sent from broker to broker. Allows broker to create a broker client connection
 * to this node's sever
 */
public record RegisterMessage(
        MessageType type,
        String name,
        InetSocketAddress address
) implements BaseMessage {

    public RegisterMessage(String name, InetSocketAddress address) {
        this(MessageType.REGISTER, name, address);
    }

    @Override
    public byte[] serialize() {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

        String host = address.getHostString();
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);

        int port = address.getPort();

        ByteBuffer buffer = ByteBuffer.allocate(
                4 + // type
                4 + nameBytes.length + // name length + bytes
                4 + hostBytes.length + // host length + bytes
                4 // port
        );

        buffer.putInt(type.getValue());

        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);

        buffer.putInt(hostBytes.length);
        buffer.put(hostBytes);

        buffer.putInt(port);

        return buffer.array();
    }

    public static RegisterMessage deserialize(ByteBuffer buffer) {
        try {
            MessageType type = MessageType.fromValue(buffer.getInt());
            if (type != MessageType.REGISTER) {
                throw new IllegalArgumentException("Invalid message type " + type);
            }

            int nameLength = buffer.getInt();
            if (nameLength < 0) {
                throw new IllegalArgumentException("Invalid name length");
            }
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);

            int hostLength = buffer.getInt();
            if (hostLength < 0) {
                throw new IllegalArgumentException("Invalid host length");
            }
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);

            int port = buffer.getInt();

            InetSocketAddress address = new InetSocketAddress(host, port);

            return new RegisterMessage(type, name, address);

        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}