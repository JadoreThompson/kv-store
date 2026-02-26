package com.zenz.kvstore;

import com.zenz.kvstore.commandHandlers.BaseCommandHandler;
import com.zenz.kvstore.commands.Command;

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
 * Protocol format: <commandType> <arg1> <arg2> <argN>
 * Examples:
 * PUT mykey myvalue
 * GET mykey
 */
public class KVServer {
    private static final int BUFFER_SIZE = 8192;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean running = true;

    private final String host;
    private final int port;

    // Track pending writes per channel
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();

    private final BaseCommandHandler commandHandler;

    public KVServer(String host, int port, BaseCommandHandler commandHandler) {
        this.commandHandler = commandHandler;
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

        System.out.println("KV Server started on " + host + ":" + port);

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
     * Process received data - parse commands and execute operations
     */
    private boolean processData(ClientSession session, ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String message = new String(bytes, StandardCharsets.UTF_8).trim();

        if (message.isEmpty()) {
            return true;
        }

        // Handle multiple commands separated by newlines
        try {
            ArrayList<Command> commands = Command.deserializeList(bytes);
            for (Command command : commands) {
                ByteBuffer responseBuffer = commandHandler.handleCommand(command);
                if (responseBuffer != null) {
                    queueWrite(session.getChannel(), responseBuffer);
                }
            }
        } catch (Exception e) {
            queueWrite(session.getChannel(), ByteBuffer.wrap(("ERROR " + e.getMessage()).getBytes(StandardCharsets.UTF_8)));
        }

        return true;
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
                queue.poll();
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

    public BaseCommandHandler getCommandHandler() {
        return commandHandler;
    }
}