package com.zenz.kvstore.server.raft;


import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @param name          Unique name for the node
 * @param serverAddress Address for the node's cluster facing server
 * @param clientAddress Address for the node's client facing server
 */
public record NodeConfig(String name, InetSocketAddress serverAddress, InetSocketAddress clientAddress) {

    public byte[] serialize() {
        final byte[] nameBytes = this.name.getBytes(StandardCharsets.UTF_8);
        final byte[] serverAddressHostBytes =
                this.serverAddress.getAddress().getHostAddress().getBytes(StandardCharsets.UTF_8);
        final byte[] clientAddressHostBytes =
                this.clientAddress.getAddress().getHostAddress().getBytes(StandardCharsets.UTF_8);
        final ByteBuffer buffer = ByteBuffer.allocate(
                4 + // Name bytes length
                        nameBytes.length + // Name bytes
                        4 + // Server serverAddress bytes length
                        serverAddressHostBytes.length + // Server serverAddress host bytes
                        4 + // Server port
                        4 + // Client serverAddress host bytes length
                        clientAddressHostBytes.length + // Client serverAddress bytes
                        4 // Client port
        );

        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        buffer.putInt(serverAddressHostBytes.length);
        buffer.put(serverAddressHostBytes);
        buffer.putInt(this.serverAddress.getPort());
        buffer.putInt(clientAddressHostBytes.length);
        buffer.put(clientAddressHostBytes);
        buffer.putInt(this.clientAddress.getPort());
        return buffer.array();
    }

    public static NodeConfig deserialize(final ByteBuffer buffer) {
        final byte[] nameBytes = new byte[buffer.getInt()];
        buffer.get(nameBytes);
        final byte[] addressHostBytes = new byte[buffer.getInt()];
        buffer.get(addressHostBytes);
        final int serverPort = buffer.getInt();
        final byte[] clientHostAddressBytes = new byte[buffer.getInt()];
        buffer.get(clientHostAddressBytes);
        final int clientPort = buffer.getInt();

        return new NodeConfig(
                new String(nameBytes, StandardCharsets.UTF_8),
                new InetSocketAddress(new String(addressHostBytes, StandardCharsets.UTF_8), serverPort),
                new InetSocketAddress(new String(clientHostAddressBytes, StandardCharsets.UTF_8), clientPort)
        );
    }

    @Override
    public String toString() {
        return "NodeConfig{" +
                "name='" + this.name + '\'' +
                ", serverAddress=" + this.serverAddress +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeConfig nodeConfig = (NodeConfig) o;
        if (this.name != null ? !name.equals(nodeConfig.name) : nodeConfig.name != null) return false;
        return serverAddress != null ? serverAddress.equals(nodeConfig.serverAddress) : nodeConfig.serverAddress == null;

    }
}
