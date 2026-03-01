package com.zenz.kvstore.raft;

import com.zenz.kvstore.*;
import com.zenz.kvstore.raft.messages.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RaftBrokerClient {
    private final InetSocketAddress remoteAddress;
    private Selector selector;
    private SocketChannel socketChannel;
    private final Queue<ByteBuffer> pendingWrites = new LinkedList<>();
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private volatile boolean isRunning;
    private final RaftManager manager;
    private final String DEBUG_PREFIX;
    private int connectionTimeout = 30000;
    private ClientStatus status = ClientStatus.DISCONNECTED;
    private boolean sentRequestVote = false;

    public RaftBrokerClient(String host, int port, RaftManager manager) {
        remoteAddress = new InetSocketAddress(host, port);
        this.manager = manager;
        DEBUG_PREFIX = String.format("[nodeId=%s RaftBrokerClient %s:%s]", manager.getConfig().id(), host, port);
    }

    public void start() throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[start] ";

        if (isRunning) {
            return;
        }

        selector = Selector.open();
        openSocketChannel();
        isRunning = true;
        long startTime = System.currentTimeMillis();
        boolean sentLeaderElectedMsg = false;

        while (isRunning) {
            int readyCount = selector.select(1000);

            NodeState state = manager.getState();
            if (state == NodeState.CANDIDATE) {
                handleIsCandidate();
            } else if (state == NodeState.CONTROLLER) {
                if (!sentLeaderElectedMsg) {
                    handleIsController();
                    sentLeaderElectedMsg = true;
                }
            }

            long elapsed = System.currentTimeMillis() - startTime;

            if (!status.equals(ClientStatus.CONNECTED) && elapsed > connectionTimeout) {
                stop();
                break;
            }

            if (readyCount == 0) {
                continue;
            }

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) {
                    continue;
                }

                try {
                    if (!status.equals(ClientStatus.CONNECTED) && key.isConnectable()) {
                        // Infinite retry
                        retryConnection(key);
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

    public void stop() throws IOException {
        if (!isRunning) return;

        isRunning = false;
        socketChannel.close();
        selector.close();
        pendingWrites.clear();
        readBuffer.clear();
        readBuffer.rewind();
    }

    private void openSocketChannel() throws IOException {
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(remoteAddress);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
    }

    private void retryConnection(SelectionKey key) throws IOException {
        if (socketChannel.isConnectionPending()) {
            try {
                socketChannel.finishConnect();
                status = ClientStatus.CONNECTED;

                int newOps = SelectionKey.OP_READ;
                if (!pendingWrites.isEmpty()) {
                    newOps |= SelectionKey.OP_WRITE;
                }
                key.interestOps(newOps);
            } catch (IOException e) {
                socketChannel.close();
                openSocketChannel();
            }
        } else {
            socketChannel.connect(remoteAddress);
        }
    }

    private void handleIsCandidate() {
        final String debugPrefix = DEBUG_PREFIX + "[handleIsCandidate] ";

        RaftManager.ElectionMeta electionMeta = manager.getElectionMeta();
        long nowPlus = System.currentTimeMillis() + 1000;

        boolean deadlineExpired = electionMeta.updateElectionDeadlineIfExpired(nowPlus);

        if (!sentRequestVote || deadlineExpired) {
            sentRequestVote = true;

            RequestVote request = new RequestVote(
                    manager.getConfig().id(),
                    electionMeta.getTerm(),
                    electionMeta.getPrevLogId(),
                    electionMeta.getPrevTerm()
            );

            queueWrite(socketChannel, ByteBuffer.wrap(request.serialize()));
        }
    }

    private void handleIsController() {
        final String debugPrefix = DEBUG_PREFIX + "[handleIsController] ";

        RaftManager.ElectionMeta electionMeta = manager.getElectionMeta();
        LeaderElected msg = new LeaderElected(
                electionMeta.getTerm(), manager.getConfig().id()
        );
        queueWrite(socketChannel, ByteBuffer.wrap(msg.serialize()));
    }

    private void handleRead(SelectionKey key) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[handleRead] ";

        SocketChannel channel = (SocketChannel) key.channel();

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

            if (bytesRead == -1) {
                stop();
                return;
            }

            if (readBuffer.position() < readBuffer.capacity() - 1) {
                break;
            }
        }

        int position = readBuffer.position();
        readBuffer.flip();
        boolean processed = processData(key);
        readBuffer.flip();
        if (processed) {
            readBuffer.compact();
        } else {
            readBuffer.position(position);
        }
    }

    private boolean processData(SelectionKey key) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[processData] ";

        BaseMessage message;
        try {
            message = BaseMessage.deserialize(readBuffer);
        } catch (IllegalArgumentException e) {
            return true;
        }

        if (message == null) {
            return false;
        }

        MessageType messageType = message.type();
        ByteBuffer responseBuffer = null;
        if (messageType == MessageType.REQUEST_VOTE_RESPONSE) {
            manager.handleVoteResponse((RequestVoteResponse) message);
        }

        if (responseBuffer != null) {
            queueWrite((SocketChannel) key.channel(), responseBuffer);
        }

        return true;
    }

    private void handleWrite(SelectionKey key) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[handleWrite] ";
        SocketChannel client = (SocketChannel) key.channel();

        if (pendingWrites.isEmpty()) {
            key.interestOps(SelectionKey.OP_READ);
            return;
        }

        ByteBuffer buffer = pendingWrites.peek();
        while (buffer != null) {
            int bytesWritten = client.write(buffer);

            if (bytesWritten == 0) {
                break;  // Send buffer full
            }

            if (!buffer.hasRemaining()) {
                pendingWrites.poll();
                buffer = pendingWrites.peek();
            }
        }

        key.interestOps(SelectionKey.OP_READ);
    }

    private void queueWrite(SocketChannel channel, ByteBuffer buffer) {
        pendingWrites.offer(buffer.duplicate());

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public RaftManager getManager() {
        return manager;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
}
