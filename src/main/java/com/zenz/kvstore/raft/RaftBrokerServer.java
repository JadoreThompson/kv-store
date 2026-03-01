package com.zenz.kvstore.raft;

import com.zenz.kvstore.KVStore;
import com.zenz.kvstore.logHandlers.RaftLogHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

public class RaftBrokerServer {
    private final String host;
    private final int port;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
    private boolean running;

    public RaftBrokerServer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() throws IOException {
        if (running) return;

        selector = Selector.open();

        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(host, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        running = true;

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
        if (!running) return;

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

        pendingWrites.clear();
    }

    private void handleAccept(SelectionKey key) throws IOException {
    }

    private void handleRead(SelectionKey key) throws IOException {
    }

    private void handleWrite(SelectionKey key) throws IOException {
    }

    private void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;

        SelectableChannel channel = key.channel();
        pendingWrites.remove(channel);
        channel.close();
        key.cancel();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return running;
    }
}
