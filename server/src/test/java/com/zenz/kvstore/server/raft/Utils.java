package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class Utils {

    private Utils() {
    }

    public static SocketChannel connectClient(InetSocketAddress address) throws IOException {
        final SocketChannel client = SocketChannel.open();
        client.configureBlocking(true);
        client.connect(address);
        return client;
    }

    public static void sendMessage(SocketChannel client, Message message) throws IOException {
        final byte[] serialized = message.serialize();
        final ByteBuffer buffer = ByteBuffer.wrap(serialized);
        while (buffer.hasRemaining()) {
            client.write(buffer);
        }
    }

    public static Message receiveMessage(SocketChannel client) throws IOException {
        ByteBuffer buffer = null;

        while (true) {
            final ByteBuffer readBuffer = ByteBuffer.allocate(1024);
            final int readCount = client.read(readBuffer);
//            System.out.println("Bytes read: " + readCount);
            if (readCount == -1) {
                break;
            }

            final ByteBuffer newBuffer = ByteBuffer.allocate(
                    (buffer == null ? 0 : buffer.capacity()) + readBuffer.capacity());
            if (buffer != null) {
                buffer.flip();
                newBuffer.put(buffer);
            }

            readBuffer.flip();
            newBuffer.put(readBuffer);
            buffer = newBuffer;

            if (readCount < readBuffer.capacity()) {
                break;
            }
        }

        buffer.flip();
        return Message.deserialize(buffer);
    }
}
