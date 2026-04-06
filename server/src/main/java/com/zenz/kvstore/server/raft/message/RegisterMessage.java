package com.zenz.kvstore.server.raft.message;

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
        InetSocketAddress serverAddress,
        InetSocketAddress clientAddress
) implements Message {

    public RegisterMessage(String name,
                           InetSocketAddress serverAddress,
                           InetSocketAddress clientAddress) {
        this(MessageType.REGISTER, name, serverAddress, clientAddress);
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
            InetSocketAddress serverAddress = new InetSocketAddress(host, port);

            int clientHostLength = buffer.getInt();
            if (clientHostLength < 0) {
                throw new IllegalArgumentException("Invalid client host length");
            }
            byte[] clientHostBytes = new byte[clientHostLength];
            buffer.get(clientHostBytes);
            String clientHost = new String(clientHostBytes, StandardCharsets.UTF_8);

            int clientPort = buffer.getInt();
            InetSocketAddress clientAddress = new InetSocketAddress(clientHost, clientPort);

            return new RegisterMessage(type, name, serverAddress, clientAddress);

        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public byte[] serialize() {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

        String host = serverAddress.getHostString();
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);
        int port = serverAddress.getPort();

        String clientHost = clientAddress.getHostString();
        byte[] clientHostBytes = clientHost.getBytes(StandardCharsets.UTF_8);
        int clientPort = clientAddress.getPort();

        ByteBuffer buffer = ByteBuffer.allocate(
                4 + // type
                        4 + nameBytes.length + // name
                        4 + hostBytes.length + // server host
                        4 + // server port
                        4 + clientHostBytes.length + // client host
                        4   // client port
        );

        buffer.putInt(type.getValue());

        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);

        // serverAddress
        buffer.putInt(hostBytes.length);
        buffer.put(hostBytes);
        buffer.putInt(port);

        // clientAddress
        buffer.putInt(clientHostBytes.length);
        buffer.put(clientHostBytes);
        buffer.putInt(clientPort);

        return buffer.array();
    }
}