package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.ClientSession;
import com.zenz.kvstore.server.raft.message.HeartbeatResponse;
import com.zenz.kvstore.server.raft.message.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

/**
 * Server implementation for a node with CONTROLLER role with its only functionality
 * to respond to heartbeat requests.
 */
public class TestControllerServer {
    private final String host = "localhost";
    private final int port;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
    private final String DBG_PREFIX = "[TestControllerServer]";
    private volatile boolean running = false;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;

    public TestControllerServer(int port) {
        this.port = port;
    }

    /**
     * Starts the server in its own thread.
     * Safe to call multiple times.
     */
    public synchronized void start() throws IOException {
        final String debugPrefix = DBG_PREFIX + "[start] ";

        System.out.println(debugPrefix + "starting test controller");
        if (running) {
            return;
        }

        running = true;
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(host, port));
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (running) {
            int readyCount = selector.select(100);
            if (readyCount == 0) continue;

            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();

                if (!key.isValid()) continue;

                try {
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    cleanup(key);
                }
            }
        }

        System.out.println(debugPrefix + "stopping");
    }

    /**
     * Stops the server whether running in a thread or not.
     */
    public void stop() throws IOException {
        final String debugPrefix = DBG_PREFIX + "[stop]";

        if (!running) {
            return;
        }

        running = false;

        for (SelectionKey key : selector.keys()) {
            try {
                cleanup(key);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        try {
            selector.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        try {
            serverSocketChannel.close();
        } catch (Exception e) {
            System.out.println(debugPrefix + "Exception during serverSocketChannel.close()");
            e.printStackTrace();
            throw e;
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        final String debugPrefix = this.DBG_PREFIX + "[handleAccept] ";
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        if (client == null) {
            return;
        }

        client.configureBlocking(false);
        client.socket().setTcpNoDelay(true);
        client.socket().setKeepAlive(true);

        SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
        clientKey.attach(new ClientSession(client));
        System.out.println(debugPrefix + "Accepted connection from " + client.getRemoteAddress());
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();
        ByteBuffer readBuffer = session.getReadBuffer();


        int readCount = channel.read(readBuffer);

        if (readCount == -1) {
            cleanup(key);
            return;
        }

        if (readCount == 0) {
            return;
        }

        int position = readBuffer.position();
        readBuffer.flip();
        boolean processed = processData(session, readBuffer);
        readBuffer.flip();

        if (processed) {
            readBuffer.clear();
        } else {
            readBuffer.position(position);
        }
    }

    private boolean processData(ClientSession session, ByteBuffer buffer) {
        final String debugPrefix = DBG_PREFIX + "[processData] ";

        Message message;
        try {
            message = Message.deserialize(buffer);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return true;
        }

        if (message == null) {
            return false;
        }

        MessageType messageType = message.type();
        ByteBuffer responseBuffer = null;

        if (messageType == MessageType.HEARTBEAT_REQUEST) {
            responseBuffer = ByteBuffer.wrap(new HeartbeatResponse().serialize());
        }

        if (responseBuffer != null) {
            queueWrite(session.getChannel(), responseBuffer);
        }

        return true;
    }

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
        final String debugPrefix = this.DBG_PREFIX + "[handleWrite] ";
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

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return serverSocketChannel != null && serverSocketChannel.isOpen();
    }
}