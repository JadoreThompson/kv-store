package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Client implementation for broker nodes
 */
public class BrokerClient {

    private final InetSocketAddress remoteAddress;
    private final Manager manager;

    private Selector selector;
    private SocketChannel socketChannel;
    private final Queue<ByteBuffer> pendingWrites = new LinkedList<>();
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private ClientStatus status = ClientStatus.DISCONNECTED;
    private boolean isRunning;

    // Debug
    private final String DEBUG_PREFIX;

    public BrokerClient(InetSocketAddress remoteAddress, Manager manager) {
        this.remoteAddress = remoteAddress;
        this.manager = manager;
        this.DEBUG_PREFIX = String.format("[%s][BrokerClient %s:%s]", this.manager.getNodeConfig().name(), remoteAddress.getHostString(), remoteAddress.getPort());
    }

    public void start() throws IOException {
        final String debugPrefix = this.DEBUG_PREFIX + "[start] ";
        if (this.isRunning) {
            return;
        }

        this.selector = Selector.open();
        openSocketChannel();
        long prevVoteRequestTerm = 0;
        boolean sentLeaderElected = false;
        boolean sentRegisterMessage = false;
        this.isRunning = true;

        while (this.isRunning) {
            final int readyCount = selector.select(100);

            // Handling roles
            final NodeRole role = manager.getRole();
            if (role == NodeRole.CANDIDATE) {
                Manager.Election election = manager.getElection();
                if (election.isExpired()) {
                    this.manager.startElection();
                    election = this.manager.getElection();
                }

                if (prevVoteRequestTerm < election.getTerm()) {
                    RequestVote request = new RequestVote(
                            manager.getNodeConfig().name(),
                            election.getTerm(),
                            election.getPrevLogId(),
                            election.getPrevTerm()
                    );
                    queueWrite(ByteBuffer.wrap(request.serialize()));
                    prevVoteRequestTerm = election.getTerm();
                    sentLeaderElected = false;
                }
            } else if (role == NodeRole.CONTROLLER && !sentLeaderElected) {
                Manager.Election election = manager.getElection();
                LeaderElected msg = new LeaderElected(election.getTerm(), manager.getNodeConfig().name());
                queueWrite(ByteBuffer.wrap(msg.serialize()));
                sentLeaderElected = true;
            }

            if (readyCount == 0) {
                continue;
            }

            final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) {
                    continue;
                }

                try {
                    if (key.isConnectable()) {
                        if (!status.equals(ClientStatus.CONNECTED)) {
                            retryConnection(key);
                        } else if (!sentRegisterMessage) {
                            // Connecting to a node within an existing cluster
                            sentRegisterMessage = true;
                            NodeConfig nodeConfig = manager.getNodeConfig();
                            queueWrite(ByteBuffer.wrap(
                                    new RegisterMessage(
                                            nodeConfig.name(), nodeConfig.serverAddress(), nodeConfig.clientAddress()
                                    ).serialize()
                            ));
                        }
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

    private void openSocketChannel() throws IOException {
        this.socketChannel = SocketChannel.open();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.connect(remoteAddress);
        this.socketChannel.register(selector, SelectionKey.OP_CONNECT);
    }

    private void retryConnection(SelectionKey key) throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[retryConnection] ";

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

    public void stop() throws IOException {
        if (!this.isRunning) {
            return;
        }

        try {
            this.isRunning = false;
            this.socketChannel.close();
            this.selector.close();
            this.pendingWrites.clear();
            this.readBuffer.clear();
            this.readBuffer.rewind();
        } finally {
            this.manager.handleBrokerClientStop(this);
        }
    }

    private void queueWrite(ByteBuffer buffer) {
        pendingWrites.offer(buffer.duplicate());

        SelectionKey key = this.socketChannel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        while (true) {
            if (readBuffer.position() >= readBuffer.capacity() - 1) {
                final int oldCap = readBuffer.capacity();
                final int newCap = (int) (oldCap * 1.6);

                ByteBuffer newReadBuffer = ByteBuffer.allocate(newCap);
                readBuffer.flip();
                newReadBuffer.put(readBuffer);
                readBuffer = newReadBuffer;
                readBuffer.flip();
            }

            final int bytesRead = channel.read(readBuffer);

            if (bytesRead == -1) {
                // Incrementing count if we're a candidate. Broker dies so majority decreases
                if (manager.getRole() == NodeRole.CANDIDATE) {
                    Manager.Election election = manager.getElection();
                    manager.handleVoteResponse(new RequestVoteResponse(true, election.getTerm()));
                }
                stop();
                return;
            }

            if (readBuffer.position() < readBuffer.capacity() - 1) {
                break;
            }
        }

        final int prevPosition = readBuffer.position();
        readBuffer.flip();
        final boolean processed = processData();
        readBuffer.flip();

        if (processed) {
            readBuffer.clear();
        } else {
            readBuffer.position(prevPosition);
        }
    }

    /**
     * Deserialises the read buffer into the message and handles it.
     *
     * @return
     */
    private boolean processData() {
        final String debugPrefix = this.DEBUG_PREFIX + "[processData] ";
        Message message;

        try {
            message = Message.deserialize(this.readBuffer);
        } catch (IllegalArgumentException e) {
            return true;
        }

        if (message == null) {
            return false;
        }

        if (message.type() == MessageType.REQUEST_VOTE_RESPONSE) {
            this.manager.handleVoteResponse((RequestVoteResponse) message);
        }

        return true;
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();

        if (this.pendingWrites.isEmpty()) {
            key.interestOps(SelectionKey.OP_READ);
            return;
        }

        ByteBuffer buffer = this.pendingWrites.peek();
        while (buffer != null) {
            int bytesWritten = client.write(buffer);

            if (bytesWritten == 0) {
                break;  // Send buffer full
            }

            if (!buffer.hasRemaining()) {
                this.pendingWrites.poll();
                buffer = this.pendingWrites.peek();
            }
        }

        key.interestOps(SelectionKey.OP_READ);
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public ClientStatus getStatus() {
        return status;
    }

    public Manager getManager() {
        return manager;
    }
}
