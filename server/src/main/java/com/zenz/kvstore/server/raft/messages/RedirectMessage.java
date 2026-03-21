package com.zenz.kvstore.server.raft.messages;

import com.zenz.kvstore.server.raft.MessageType;
import com.zenz.kvstore.server.raft.NodeRole;
import com.zenz.kvstore.server.raft.NodeConfig;

import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record RedirectMessage(
        MessageType type,
        NodeConfig node,
        NodeRole role
) implements BaseMessage {

    public RedirectMessage(NodeConfig node, NodeRole role) {
        this(MessageType.REDIRECT, node, role);
    }


    @Override
    public byte[] serialize() {
        byte[] nameBytes = node.name().getBytes(StandardCharsets.UTF_8);

        String host = node.address().getHostString();
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);

        int port = node.address().getPort();

        ByteBuffer buffer = ByteBuffer.allocate(
                4 +                 // message type
                        4 + nameBytes.length + // node name length + bytes
                        4 + hostBytes.length + // host length + bytes
                        4 +                 // port
                        4                   // role
        );

        buffer.putInt(type.getValue());

        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);

        buffer.putInt(hostBytes.length);
        buffer.put(hostBytes);

        buffer.putInt(port);
        buffer.putInt(role.getValue());

        return buffer.array();
    }

    public static RedirectMessage deserialize(ByteBuffer buffer) {
        try {
            MessageType type = MessageType.fromValue(buffer.getInt());
            if (type != MessageType.REDIRECT) {
                throw new IllegalArgumentException("Invalid response type " + type);
            }

            int nameLength = buffer.getInt();
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);

            int hostLength = buffer.getInt();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);

            int port = buffer.getInt();
            NodeRole role = NodeRole.fromValue(buffer.getInt());

            NodeConfig node = new NodeConfig(name, new InetSocketAddress(host, port));
            return new RedirectMessage(type, node, role);

        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}