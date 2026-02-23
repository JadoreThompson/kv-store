package com.zenz.kvstore;

import com.zenz.kvstore.messages.Message;
import com.zenz.kvstore.messages.PingResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class KVRaftController {
    private final int heartbeatIntervalMs;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private HashMap<SocketChannel, Queue<ByteBuffer>> pendingWrites;
    private boolean running;

    public KVRaftController(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        pendingWrites = new HashMap<>();
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
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String message = new String(bytes, StandardCharsets.UTF_8).trim();

        if (message.isEmpty()) return;

        String[] lines = message.split("\n");
        for (String line : lines) {
            if (line.isBlank()) continue;

            System.out.println("Processing message: " + line);
            String response = processMessage(line.trim());
            System.out.println("Response: " + response);

            ByteBuffer responseBuffer = ByteBuffer.wrap((response + "\n").getBytes(StandardCharsets.UTF_8));
            queueWrite(session.getClient(), responseBuffer);
        }
    }

    private String processMessage(String line) {
        Message message;
        try {
            message = Message.fromString(line);
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage();
        }

        if (message.type().equals(MessageType.PING_REQUEST)) return new PingResponse().toString();

        return "ERROR: Unsupported message type " + message.type().toString();
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

    private static class ClientSession {
        private static final int BUFFER_SIZE = 8192;

        private final SocketChannel client;
        private final ByteBuffer readBuffer;

        ClientSession(SocketChannel client) {
            this.client = client;
            this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        }

        SocketChannel getClient() {
            return client;
        }

        ByteBuffer getReadBuffer() {
            return readBuffer;
        }
    }
}
