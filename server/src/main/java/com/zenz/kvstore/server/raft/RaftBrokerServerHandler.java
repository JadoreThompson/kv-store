package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.ClientSession;
import com.zenz.kvstore.server.MessageType;
import com.zenz.kvstore.server.SocketHandler;
import com.zenz.kvstore.server.SocketServer;
import com.zenz.kvstore.server.raft.messages.BaseMessage;
import com.zenz.kvstore.server.raft.messages.LeaderElected;
import com.zenz.kvstore.server.raft.messages.RequestVote;
import com.zenz.kvstore.server.raft.messages.RequestVoteResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

public class RaftBrokerServerHandler implements SocketHandler {
    private Selector selector;
    private Map<SocketChannel, Queue<ByteBuffer>> pendingWrites;
    private RaftManager manager;
    private SocketServer server;
    private final String DEBUG_PREFIX;

    public RaftBrokerServerHandler(SocketServer server, RaftManager manager) {
        this.manager = manager;
        this.server = server;
        DEBUG_PREFIX = String.format("[nodeId=%s RaftBrokerServer]", manager.getConfig().id());
    }

    public void init() throws Exception {
        selector = server.getSelector();
        pendingWrites = server.getPendingWrites();
    }

    public void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();

        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
        channel.keyFor(selector).attach(new ClientSession(channel));
    }

    public void handleRead(SelectionKey key) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[handleRead] ";

        ClientSession session = (ClientSession) key.attachment();
        ByteBuffer readBuffer = session.getReadBuffer();
        SocketChannel channel = (SocketChannel) key.channel();
        int totalBytesRead = 0;

        while (true) {
            if (readBuffer.position() >= readBuffer.capacity() - 1) {
                int oldCap = readBuffer.capacity();
                int newCap = (int) (oldCap * 1.6);

                ByteBuffer newReadBuffer = ByteBuffer.allocate(newCap);
                readBuffer.flip();
                newReadBuffer.put(readBuffer);
                readBuffer = newReadBuffer;
                readBuffer.flip();
            }

            int bytesRead = channel.read(readBuffer);
            totalBytesRead += bytesRead;

            if (bytesRead == -1) {
                server.cleanup(key);
                return;
            }

            if (bytesRead == 0) {
                return;
            }

            if (readBuffer.position() < readBuffer.capacity() - 1) {
                break;
            }
        }

        if (totalBytesRead == 0) return;

        session.setReadBuffer(readBuffer);
        int prevPosition = readBuffer.position();

        readBuffer.flip();
        boolean processed = processData(key, readBuffer);
        readBuffer.flip();

        if (processed) {
            readBuffer.clear();
            readBuffer.rewind();
        } else {
            readBuffer.position(prevPosition);
        }
    }

    private boolean processData(SelectionKey key, ByteBuffer readBuffer) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[processData] ";

        BaseMessage message;
        try {
            message = BaseMessage.deserialize(readBuffer);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return true;
        }

        if (message == null) {
            return false;
        }

        ByteBuffer responseBuffer = null;
        MessageType messageType = message.type();

        if (messageType.equals(MessageType.REQUEST_VOTE)) {
            RequestVote msg = (RequestVote) message;
            manager.setLastTerm(msg.term());
            boolean shouldGrant = manager.shouldGrantVote((RequestVote) message);

            if (shouldGrant) {
                RequestVoteResponse response = new RequestVoteResponse(
                        true, ((RequestVote) message).term()
                );

                responseBuffer = ByteBuffer.wrap(response.serialize());
            }

        } else if (messageType.equals(MessageType.LEADER_ELECTED)) {

            LeaderElected msg = (LeaderElected) message;
            manager.setLastTerm(msg.term());
            manager.handleLeaderElected((LeaderElected) message);

            server.cleanup(key);
        }

        if (responseBuffer != null) {
            queueWrite((SocketChannel) key.channel(), responseBuffer);
        }

        return true;
    }

    private void queueWrite(SocketChannel channel, ByteBuffer buffer) {
        final String debugPrefix = DEBUG_PREFIX + "[queueWrite] ";

        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(buffer.duplicate());

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    public void handleWrite(SelectionKey key) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[handleWrite] ";
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
}
