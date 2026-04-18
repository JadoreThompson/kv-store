package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.InvalidMessageException;
import com.zenz.kvstore.server.raft.message.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.function.Function;

@Slf4j
public class TestRaftServer implements AutoCloseable {

    private Selector selector;
    private ServerSocketChannel socketChannel;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
    @Getter
    private boolean isRunning;
    @Getter
    @Setter
    private Function<Message, ByteBuffer> messageHandler;

    @Getter
    private final InetSocketAddress address;

    public TestRaftServer(final InetSocketAddress address) {
        this.address = address;
    }

    public void open() throws IOException {
        if (isRunning) {
            return;
        }

        isRunning = true;
        selector = Selector.open();
        socketChannel = ServerSocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.bind(address);
        socketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (isRunning) {
            final int readyCount = selector.select();
            if (readyCount == 0) {
                continue;
            }
            
            final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                final SelectionKey key = it.next();
                it.remove();

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
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!isRunning) {
            return;
        }

        isRunning = false;
        if (selector != null) {
            for (SelectionKey key : selector.keys()) {
                cleanup(key);
            }
            selector.close();
        }
        if (socketChannel != null) {
            socketChannel.close();
        }
    }

    private void handleAccept(final SelectionKey key) throws IOException {
        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel client = server.accept();

        if (client == null) {
            return;
        }

        client.configureBlocking(false);
        client.socket().setTcpNoDelay(true);
        client.socket().setKeepAlive(true);

        SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
        clientKey.interestOps(SelectionKey.OP_READ);
        clientKey.attach(new Session(client));
    }

    private void handleRead(final SelectionKey key) throws IOException {
        final SocketChannel client = (SocketChannel) key.channel();
        final Session session = (Session) key.attachment();

        int totalRead = 0;
        int readCount = 0;
        do {
            if (session.buffer.position() == session.buffer.limit()) {
                final ByteBuffer buffer = ByteBuffer.allocate(session.buffer.capacity() * 2);
                final int prevPosition = session.buffer.position();
                buffer.put(session.buffer.flip());
                buffer.flip();
                buffer.limit(buffer.capacity());
                buffer.position(prevPosition);
                session.buffer = buffer;
            }

            readCount = client.read(session.buffer);
            totalRead += readCount;

            if (readCount == -1) {
                cleanup(key);
            }
        } while (readCount > 0);

        if (totalRead == 0) {
            return;
        }

        final int prevPosition = session.buffer.position();
        session.buffer.flip();
        final boolean processed = processData(session);

        if (processed) {
            session.buffer.clear();
            session.buffer = ByteBuffer.allocate(1024);
        } else {
            session.buffer.flip();
            session.buffer.limit(session.buffer.capacity());
            session.buffer.position(prevPosition);
        }
    }

    private void cleanup(final SelectionKey key) throws IOException {
        if (key == null || !key.isValid()) {
            return;
        }

        final SelectableChannel client = key.channel();
        pendingWrites.remove(client);
        key.cancel();

        try {
            client.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    private boolean processData(final Session session) throws IOException {
        if (messageHandler == null) {
            return true;
        }

        Message message;
        try {
            message = Message.deserialize(session.buffer);
        } catch (BufferUnderflowException | InvalidMessageException e) {
            return false;
        }

        ByteBuffer responseBuffer = messageHandler.apply(message);
        if (responseBuffer != null) {
            queueWrite(session.channel, responseBuffer);
        }

        return true;
    }

    private void queueWrite(final SocketChannel channel, final ByteBuffer buffer) {
        final Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(buffer.duplicate());

        final SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void handleWrite(final SelectionKey key) throws IOException {
        final Session session = (Session) key.attachment();
        if (session == null) {
            return;
        }

        final Queue<ByteBuffer> queue = pendingWrites.get(session.channel);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        final ByteBuffer buffer = queue.peek();
        while (buffer.hasRemaining()) {
            final int writeCount = session.channel.write(buffer);
            if (writeCount == 0) {
                break;
            }
        }

        if (!buffer.hasRemaining()) {
            queue.poll();
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        } else {
        }
    }

    private static class Session {

        public final SocketChannel channel;
        public ByteBuffer buffer = ByteBuffer.allocate(1024);

        public Session(final SocketChannel channel) {
            this.channel = channel;
        }
    }
}
