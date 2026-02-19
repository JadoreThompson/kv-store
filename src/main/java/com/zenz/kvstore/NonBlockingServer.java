package com.zenz.kvstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Production-ready non-blocking TCP server using NIO Selector.
 * Handles multiple concurrent connections with a single thread.
 */
public class NonBlockingServer {

    private static final int BUFFER_SIZE = 8192;
    private static final int PORT = 8080;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean running = true;

    // Track partial writes per channel
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();

    public static void main(String[] args) {
        NonBlockingServer server = new NonBlockingServer();
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {
        // 1. Open selector (epoll/kqueue/IOCP depending on OS)
        selector = Selector.open();

        // 2. Create server socket channel
        serverChannel = ServerSocketChannel.open();

        // 3. CRITICAL: Configure non-blocking mode
        serverChannel.configureBlocking(false);

        // 4. Bind to port
        serverChannel.socket().bind(new InetSocketAddress(PORT));

        // 5. Register with selector for ACCEPT events
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Server started on port " + PORT);
        System.out.println("Using selector: " + selector.getClass().getSimpleName());

        // 6. Main event loop
        while (running) {
            // Block until at least one channel is ready
            // Returns number of ready keys, or 0 if woken up by wakeup()
            int readyCount = selector.select();

            if (readyCount == 0) continue;

            // Get iterator over ready keys (MUST use iterator for removal)
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove(); // CRITICAL: Remove from selected set

                try {
                    if (!key.isValid()) {
                        cleanup(key);
                        continue;
                    }

                    // Handle different ready operations
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    }

                    if (key.isReadable()) {
                        handleRead(key);
                    }

                    if (key.isWritable()) {
                        handleWrite(key);
                    }

                } catch (IOException e) {
                    // Connection error - cleanup
                    System.err.println("Connection error: " + e.getMessage());
                    cleanup(key);
                }
            }
        }
    }

    /**
     * ACCEPT: New client connection available
     */
    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();

        // Non-blocking accept - returns immediately
        SocketChannel client = server.accept();

        if (client == null) {
            // Spurious wakeup, no connection actually ready
            return;
        }

        // CRITICAL: Set client to non-blocking
        client.configureBlocking(false);

        // Optional tuning
        client.socket().setTcpNoDelay(true); // Disable Nagle's algorithm
        client.socket().setKeepAlive(true);

        // Register for READ (and potentially WRITE)
        // We don't register OP_WRITE initially - only when we have data to write
        SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);

        // Attach session data if needed
        clientKey.attach(new ClientSession(client));

        System.out.println("Client connected: " + client.getRemoteAddress());
    }

    /**
     * READ: Data available to read from client
     */
    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();

        ByteBuffer buffer = session.getReadBuffer();

        // Non-blocking read - returns immediately with bytes read
        // Could be: >0 (bytes read), 0 (no data), -1 (EOF/closed)
        int bytesRead = client.read(buffer);

        if (bytesRead == -1) {
            // Client closed connection gracefully
            System.out.println("Client disconnected: " + client.getRemoteAddress());
            cleanup(key);
            return;
        }

        if (bytesRead == 0) {
            // No data available (spurious wakeup)
            return;
        }

        // Process the data
        buffer.flip(); // Switch from write mode to read mode
        boolean processed = processData(session, buffer);
        buffer.compact(); // Preserve remaining partial data

        if (!processed) {
            // Protocol error or malicious data
            cleanup(key);
        }
    }

    /**
     * Process received data (example: echo server with simple protocol)
     */
    private boolean processData(ClientSession session, ByteBuffer buffer) {
        // Example: Simple line-based protocol
        // Look for \n to indicate complete message

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String message = new String(bytes, StandardCharsets.UTF_8).trim();

        System.out.println("Received from " + session.getClient() + ": " + message);

        // Echo back with modification
        String response = "Echo: " + message + "\n";
        ByteBuffer responseBuffer = ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8));

        // Queue write - may not all go out immediately
        queueWrite(session.getClient(), responseBuffer);

        return true;
    }

    /**
     * Queue data for writing and enable OP_WRITE interest
     */
    private void queueWrite(SocketChannel client, ByteBuffer data) {
        // Get or create write queue for this client
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(client, k -> new LinkedList<>());

        // Duplicate buffer to avoid position/limit issues
        queue.offer(data.duplicate());

        // Find the key and enable WRITE interest
        SelectionKey key = client.keyFor(selector);
        if (key != null && key.isValid()) {
            // Add OP_WRITE to interest set
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

            // CRITICAL: wakeup selector if it's blocked in select()
            // The interestOps change is deferred until next select(),
            // but we want it now if we're about to write
            selector.wakeup();
        }
    }

    /**
     * WRITE: Channel is ready to accept writes (send buffer has space)
     */
    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        Queue<ByteBuffer> queue = pendingWrites.get(client);

        if (queue == null || queue.isEmpty()) {
            // Nothing to write, disable OP_WRITE to avoid busy-looping
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }

        // Write as much as possible from queue
        ByteBuffer buffer = queue.peek();
        while (buffer != null) {
            int bytesWritten = client.write(buffer);

            if (bytesWritten == 0) {
                // Kernel send buffer full, stop here
                // OP_WRITE will trigger again when space available
                break;
            }

            if (!buffer.hasRemaining()) {
                // Finished this buffer, remove and continue
                queue.poll();
                buffer = queue.peek();
            }
        }

        // If queue empty, disable OP_WRITE interest
        if (queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    /**
     * Cleanup resources for a key
     */
    private void cleanup(SelectionKey key) {
        try {
            key.cancel(); // Remove from selector

            SocketChannel client = (SocketChannel) key.channel();
            pendingWrites.remove(client);

            try {
                client.close();
            } catch (IOException e) {
                // Ignore close errors
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        running = false;
        selector.wakeup(); // Unblock select() if waiting
    }

    /**
     * Per-client session state
     */
    private static class ClientSession {
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