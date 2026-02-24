package com.zenz.kvstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Non-blocking TCP server for KVStore.
 * Handles multiple concurrent connections using NIO Selector.
 * <p>
 * Protocol format: <operationType> <arg1> <arg2> <argN>
 * Examples:
 * PUT mykey myvalue
 * GET mykey
 * <p>
 * Uses CommandProcessor to handle operations, allowing for both
 * single-node (DirectCommandProcessor) and Raft (KVRaftCommandProcessor) modes.
 */
public class KVConnectionManager {
    private static final int BUFFER_SIZE = 8192;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean running = true;

    private final CommandProcessor processor;
    private final String host;
    private final int port;

    // Track pending writes per channel
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();

    /**
     * Creates a KVConnectionManager with a CommandProcessor.
     * Use this constructor for flexibility (single-node or Raft mode).
     *
     * @param host     the host address to bind to
     * @param port     the port to listen on
     * @param processor the CommandProcessor to handle operations
     */
    public KVConnectionManager(String host, int port, CommandProcessor processor) {
        this.processor = processor;
        this.host = host;
        this.port = port;
    }

    /**
     * Legacy constructor for backward compatibility.
     * Creates a DirectCommandProcessor wrapping the store.
     *
     * @param host  the host address to bind to
     * @param port  the port to listen on
     * @param store the KVStore to use
     */
    public KVConnectionManager(String host, int port, KVStore store) {
        this.processor = new DirectCommandProcessor(store);
        this.host = host;
        this.port = port;
    }

    public void start() throws IOException {
        // Open selector
        selector = Selector.open();

        // Create and configure server channel
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(host, port));

        // Register for ACCEPT events
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("KV Connection Manager server started on " + host + ":" + port);

        // Main event loop
        while (running) {
            int readyCount = selector.select();

            if (readyCount == 0) continue;

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();


                try {
                    if (!key.isValid()) {
                        cleanup(key);
                    } else if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                } catch (IOException e) {
                    System.err.println("Connection error: " + e.getMessage());
                    cleanup(key);
                }
            }
        }

    }

    public void stop() throws IOException {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }

        if (selector != null) {
            for (SelectionKey key : selector.keys()) {
                cleanup(key);
            }
            selector.close();
        }

        if (serverChannel != null) {
            serverChannel.close();
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();

        if (client == null) {
            return;
        }

        client.configureBlocking(false);
        client.socket().setTcpNoDelay(true);
        client.socket().setKeepAlive(true);

        // Register for READ events with session attachment
        SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
        clientKey.attach(new ClientSession(client));
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();

        ByteBuffer buffer = session.getReadBuffer();
        int bytesRead = client.read(buffer);

        if (bytesRead == -1) {
            cleanup(key);
            return;
        }

        if (bytesRead == 0) {
            return;
        }

        // Process the data
        buffer.flip();
        boolean processed = processData(session, buffer);
        buffer.compact();

        if (!processed) {
            cleanup(key);
        }
    }

    /**
     * Process received data - parse commands and submit to CommandProcessor.
     * Uses async callbacks to avoid blocking the NIO thread.
     */
    private boolean processData(ClientSession session, ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String message = new String(bytes, StandardCharsets.UTF_8).trim();

        if (message.isEmpty()) {
            return true;
        }

        // Handle multiple commands separated by newlines
        String[] lines = message.split("\n");

        for (String line : lines) {
            if (line.isBlank()) continue;

            System.out.println("Received command: " + line);
            processCommandAsync(session, line.trim());
        }

        return true;
    }

    /**
     * Process a single command asynchronously using the CommandProcessor.
     * The response is queued for write when the future completes.
     */
    private void processCommandAsync(ClientSession session, String line) {
        String[] parts = line.split(" ");

        if (parts.length == 0) {
            sendErrorResponse(session, "ERROR: Empty command");
            return;
        }

        String operation = parts[0].toUpperCase();

        try {
            // Handle PING synchronously (no store interaction needed)
            if (operation.equals("PING")) {
                ByteBuffer responseBuffer = ByteBuffer.wrap("PONG\n".getBytes(StandardCharsets.UTF_8));
                queueWrite(session.getClient(), responseBuffer);
                return;
            }

            OperationType opType = OperationType.valueOf(operation);

            switch (opType) {
                case PUT -> handlePutAsync(session, parts);
                case GET -> handleGetAsync(session, parts);
                default -> sendErrorResponse(session, "ERROR: Unknown operation '" + operation + "'");
            }
        } catch (IllegalArgumentException e) {
            sendErrorResponse(session, "ERROR: Unknown operation '" + operation + "'");
        }
    }

    /**
     * Handle PUT command asynchronously: PUT <key> <value>
     */
    private void handlePutAsync(ClientSession session, String[] parts) {
        if (parts.length < 3) {
            sendErrorResponse(session, "ERROR: PUT requires <key> and <value>");
            return;
        }

        String key = parts[1];

        // Join remaining parts as value (in case value contains spaces)
        StringBuilder valueBuilder = new StringBuilder();
        for (int i = 2; i < parts.length; i++) {
            if (i > 2) valueBuilder.append(" ");
            valueBuilder.append(parts[i]);
        }

        byte[] value = valueBuilder.toString().getBytes(StandardCharsets.UTF_8);

        try {
            processor.handlePut(key, value)
                .thenAccept(responseBuffer -> {
                    // Add newline and queue for write
                    byte[] respArr = responseBuffer.array();
                    ByteBuffer wrapped = ByteBuffer.allocate(respArr.length + 1);
                    wrapped.put(respArr);
                    wrapped.put((byte) '\n');
                    wrapped.flip();
                    
                    queueWrite(session.getClient(), wrapped);
                    System.out.println("Sending response: " + new String(wrapped.array(), StandardCharsets.UTF_8).trim());
                })
                .exceptionally(ex -> {
                    System.err.println("Error processing PUT: " + ex.getMessage());
                    sendErrorResponse(session, "ERROR: " + ex.getMessage());
                    return null;
                });
        } catch (IOException e) {
            sendErrorResponse(session, "ERROR: " + e.getMessage());
        }
    }

    /**
     * Handle GET command asynchronously: GET <key>
     */
    private void handleGetAsync(ClientSession session, String[] parts) {
        if (parts.length < 2) {
            sendErrorResponse(session, "ERROR: GET requires <key>");
            return;
        }

        String key = parts[1];

        try {
            processor.handleGet(key)
                .thenAccept(responseBuffer -> {
                    // Add newline and queue for write
                    byte[] respArr = responseBuffer.array();
                    ByteBuffer wrapped = ByteBuffer.allocate(respArr.length + 1);
                    wrapped.put(respArr);
                    wrapped.put((byte) '\n');
                    wrapped.flip();
                    
                    queueWrite(session.getClient(), wrapped);
                    System.out.println("Sending response: " + new String(wrapped.array(), StandardCharsets.UTF_8).trim());
                })
                .exceptionally(ex -> {
                    System.err.println("Error processing GET: " + ex.getMessage());
                    sendErrorResponse(session, "ERROR: " + ex.getMessage());
                    return null;
                });
        } catch (IOException e) {
            sendErrorResponse(session, "ERROR: " + e.getMessage());
        }
    }

    /**
     * Send an error response to the client
     */
    private void sendErrorResponse(ClientSession session, String error) {
        ByteBuffer responseBuffer = ByteBuffer.wrap((error + "\n").getBytes(StandardCharsets.UTF_8));
        queueWrite(session.getClient(), responseBuffer);
    }

    /**
     * Queue data for writing and enable OP_WRITE interest
     */
    private void queueWrite(SocketChannel client, ByteBuffer data) {
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(client, k -> new LinkedList<>());
        queue.offer(data.duplicate());

        SelectionKey key = client.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        Queue<ByteBuffer> queue = pendingWrites.get(client);

        if (queue == null || queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }

        ByteBuffer buffer = queue.peek();
        while (buffer != null) {
            int bytesWritten = client.write(buffer);

            if (bytesWritten == 0) {
                break;  // Send buffer full
            }

            if (!buffer.hasRemaining()) {
                queue.poll(); // Advanced head to next element
                buffer = queue.peek();
            }
        }

        if (queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    private void cleanup(SelectionKey key) {
        if (key == null || !key.isValid()) return;

        try {

            SelectableChannel client = key.channel();
            pendingWrites.remove(client);

            key.cancel();
            try {
                client.close();
            } catch (IOException e) {
                // Ignore
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
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