package com.zenz.kvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class KVRaftController {
    private final int heartbeatIntervalMs;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private HashMap<SelectionKey, Queue<ByteBuffer>> pendingWrites;
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
    }

    public void handleRead(SelectionKey key) throws IOException {
    }

    public void handleWrite(SelectionKey key) throws IOException {
    }

    public void cleanup(SelectionKey key) throws IOException {
    }
}
