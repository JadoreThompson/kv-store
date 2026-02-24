package com.zenz.kvstore;

import com.zenz.kvstore.messages.Message;
import com.zenz.kvstore.messages.PingResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class KVRaft {
    private KVRaftRole role;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private HashMap<SocketChannel, Queue<ByteBuffer>> pendingWrites;
    private boolean running;

    public KVRaft(KVRaftRole role) throws IOException {
        pendingWrites = new HashMap<>();
        this.role = role;
    }

    public void start() throws IOException {
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (running) {
            selector.select();
            Iterator<SelectionKey> keysIter = selector.selectedKeys().iterator();

            while (keysIter.hasNext()) {
                SelectionKey key = keysIter.next();
                keysIter.remove();

                if (!key.isValid()) continue;

                try {
                    if (key.isAcceptable()) handleAccept(key);
                    else if (key.isReadable()) handleRead(key);
                    else if (key.isWritable()) handleWrite(key);
                } catch (Exception e) {
                    cleanup(key);
                    e.printStackTrace();
                }
            }
        }
    }

    public void stop() throws IOException {
        if (!running) return;

        for (SelectionKey key : selector.keys()) {
            if (!key.isValid()) continue;
            cleanup(key);
        }

        selector.close();
        serverSocketChannel.close();
    }

    public void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel channel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = channel.accept();

        socketChannel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
    }

    public void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();

        ByteBuffer buffer = session.getReadBuffer();
        int bytesRead = channel.read(buffer);

        // Client disconnected
        if (bytesRead == -1) {
            cleanup(key);
            return;
        }

        if (bytesRead == 0) return;

        buffer.flip();
        processData(session, buffer);
        buffer.compact();
    }

    private void processData(ClientSession session, ByteBuffer buffer) throws IOException {
        int currentPosition = buffer.position();
        buffer.rewind();

        Message message;

        try {
            message = Message.deserialize(buffer);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            // Resetting position to original position as we assume there weren't
            // enough bytes within the buffer to form a coherent message
            buffer.position(currentPosition);
            return;
        }

        // Clearing current message from buffer
        buffer.clear();
        ByteBuffer respBuffer;
        if (message.type().equals(MessageType.PING_REQUEST)) {
            respBuffer = ByteBuffer.wrap(new PingResponse().serialize());
        } else {
            respBuffer = ByteBuffer.wrap("ERROR: Unknown operation type".getBytes(StandardCharsets.UTF_8));
        }

        queueWrite(session.getChannel(), respBuffer);
    }

    private void queueWrite(SocketChannel channel, ByteBuffer data) {
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(data.duplicate());

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    public void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Queue<ByteBuffer> queue = pendingWrites.get(channel);

        if (queue == null || queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }

        ByteBuffer buffer = queue.peek(); // Retrieve head
        while (buffer != null) {
            int bytesWritten = channel.write(buffer);

            if (bytesWritten == 0) {
                break;  // Send buffer full
            }

            if (!buffer.hasRemaining()) {
                // Retrieve and remove head, advancing head to the next element
                queue.poll();
                buffer = queue.peek();
            }
        }

        if (queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    public void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;
        SelectableChannel channel = key.channel();
        pendingWrites.remove(channel);

        key.cancel();

        try {
            channel.close();
        } catch (IOException e) {
        }
    }

    public KVRaftRole getRole() {
        return role;
    }

    private static class ClientSession {
        private static final int BUFFER_SIZE = 8192;

        private final SocketChannel channel;
        private final ByteBuffer readBuffer;

        ClientSession(SocketChannel client) {
            this.channel = client;
            this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        }

        SocketChannel getChannel() {
            return channel;
        }

        ByteBuffer getReadBuffer() {
            return readBuffer;
        }
    }
}
