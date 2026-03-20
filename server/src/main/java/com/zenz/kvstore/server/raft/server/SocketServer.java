package com.zenz.kvstore.server.raft.server;

import com.zenz.kvstore.server.raft.server.handlers.SocketHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class SocketServer {
    private final String host;
    private final int port;
    private volatile SocketHandler socketHandler;
    private boolean isRunning = false;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();

    private final String DEBUG_PREFIX;

    public SocketServer(String host, int port) {
        this.host = host;
        this.port = port;
        DEBUG_PREFIX = String.format("[SocketServer %s:%s]", host, port);
    }

    public void start() throws Exception {
        if (isRunning) return;

        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(host, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        isRunning = true;
        socketHandler.init();

        while (isRunning) {
            int readyCount = selector.select();
            socketHandler.handleWakeUp();
            if (readyCount == 0) {
                continue;
            }

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                try {
                    if (!key.isValid()) {
                        cleanup(key);
                    } else if (key.isAcceptable()) {
                        socketHandler.handleAccept(key);
                    } else if (key.isReadable()) {
                        socketHandler.handleRead(key);
                    } else if (key.isWritable()) {
                        socketHandler.handleWrite(key);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    cleanup(key);
                }
            }
        }
    }

    public void stop() throws IOException {
        if (!isRunning) {
            return;
        }

        isRunning = false;

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

    public void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;

        SelectableChannel channel = key.channel();
        channel.close();
        key.cancel();
        pendingWrites.remove(channel);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public SocketHandler getSocketHandler() {
        return socketHandler;
    }

    public void setSocketHandler(SocketHandler socketHandler) {
        this.socketHandler = socketHandler;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public Selector getSelector() {
        return selector;
    }

    public ServerSocketChannel getServerChannel() {
        return serverChannel;
    }

    public Map<SocketChannel, Queue<ByteBuffer>> getPendingWrites() {
        return pendingWrites;
    }

    public void setPendingWrites(Map<SocketChannel, Queue<ByteBuffer>> pendingWrites) {
        this.pendingWrites = pendingWrites;
    }
}
