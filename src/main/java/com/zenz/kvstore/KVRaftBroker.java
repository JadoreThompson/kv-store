package com.zenz.kvstore;

import com.zenz.kvstore.messages.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Brokers are both servers and clients. They must be servers
 * in order to be discovered and clients to interact with the controller.
 */
public class KVRaftBroker {
    private final long brokerId;
    private final KVRaftStore store;
    private final String host;
    private final int port;
    private ArrayList<InetSocketAddress> peerAddrs = new ArrayList<>();
    private InetSocketAddress controllerAddr;

    // Server
    private ServerSocketChannel serverSocketChannel;
    private Selector serverSelector;
    private HashMap<SocketChannel, Queue<ByteBuffer>> pendingWrites;
    // Controller Client
    private SocketChannel clientSocketChannel;
    private Selector clientSelector;

    private boolean running = false;

    // -1: Not looking for votes
    // >= 0: Looking for votes
    private int voteCount = -1;
    private int majority = -1;

    public KVRaftBroker(long brokerId, KVRaftStore store, String host, int port, InetSocketAddress controllerAddr) {
        this.brokerId = brokerId;
        this.store = store;
        this.host = host;
        this.port = port;
        this.controllerAddr = controllerAddr;
    }

    /**
     * Adds to the peer brokers list. Call this method
     * as many times as needed prior to the start method.
     *
     * @param address
     * @return
     */
    public KVRaftBroker addPeer(InetSocketAddress address) {
        if (!running) peerAddrs.add(address);
        return this;
    }

    /**
     * Launches the server and initiates connection to the controller
     *
     * @throws IOException
     */
    public void start() throws IOException {
        connectToController();
        startServer();
    }

    public void stop() {
    }


    private void startServer() throws IOException {
        if (running) return;

        serverSelector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(serverSelector, SelectionKey.OP_ACCEPT);

        running = true;

        while (running) {
            int readyCount = serverSelector.select();
            if (readyCount == 0) continue;

            Iterator<SelectionKey> keysIterator = serverSelector.selectedKeys().iterator();
            while (keysIterator.hasNext()) {
                SelectionKey key = keysIterator.next();
                keysIterator.remove();
                if (!key.isValid()) continue;

                try {
                    if (key.isAcceptable()) handleAccept(key);
                    else if (key.isReadable()) handleRead(key);
                    else if (key.isWritable()) handleWrite(key);
                } catch (Exception e) {
                    e.printStackTrace();
                    cleanup(key);
                }
            }
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel channel = server.accept();

        if (channel == null) {
            return;
        }

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setKeepAlive(true);

        // Register for READ events with session attachment
        SelectionKey clientKey = channel.register(serverSelector, SelectionKey.OP_READ);
        clientKey.attach(new ClientSession(channel));
    }

    /**
     * Expecting a VoteRequest, VoteResponse
     *
     * @param key
     * @throws IOException
     */
    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();
        ByteBuffer buffer = session.getReadBuffer();

        int readCount = channel.read(buffer);

        if (readCount == -1) cleanup(key); // Client disconnected
        if (readCount == 0) return;

        int position = buffer.position();

        try {
            buffer.rewind();
            buffer.flip();

//            long curTerm = store.getTerm();
//            long curLogId = store.getLogId();
//            VoteRequest request = VoteRequest.deserialize(buffer);
//            boolean voteGranted = false;
//
//            if (
//                    (request.term() == store.getTerm() + 1) ||
//                            (request.term() == curTerm && request.logId() > curLogId)
//            ) {
//                voteGranted = true;
//            }
//
//            VoteResponse response = new VoteResponse(request.term(), voteGranted);
//            ByteBuffer responseBuffer = ByteBuffer.wrap(response.serialize());

            Message msg = Message.deserialize(buffer);
            ByteBuffer responseBuffer;
            if (msg.type().equals(MessageType.VOTE_REQUEST)) {
                responseBuffer = handleVoteRequest((VoteRequest) msg);
            } else if (msg.type().equals(MessageType.VOTE_RESPONSE)) {
                responseBuffer = handleVoteResponse((VoteResponse) msg);
            } else if (msg.type().equals(MessageType.LEADER_ELECTED)) {
                responseBuffer = handleLeaderElected((LeaderElected) msg);
            } else {
                responseBuffer = ByteBuffer.wrap(("ERROR Invalid message type " + msg.type().name()).getBytes(StandardCharsets.UTF_8));
            }

            queueWrite(channel, responseBuffer);

            buffer.clear();
            buffer.flip();
        } catch (IllegalArgumentException e) {
            // Ignore
            buffer.position(position);
            buffer.flip();
        }
    }

    public ByteBuffer handleVoteRequest(VoteRequest request) {
        long curTerm = store.getTerm();
        long curLogId = store.getLogId();
        boolean voteGranted = false;

        if (
                (request.term() == store.getTerm() + 1) ||
                        (request.term() == curTerm && request.logId() > curLogId)
        ) {
            voteGranted = true;
        }

        VoteResponse response = new VoteResponse(request.term(), voteGranted);
        return ByteBuffer.wrap(response.serialize());
    }

    public ByteBuffer handleVoteResponse(VoteResponse response) {
        if (voteCount == -1) return ByteBuffer.wrap("ERROR Not a candidate".getBytes(StandardCharsets.UTF_8));
        if (!response.voteGranted()) return ByteBuffer.wrap(new byte[0]);

        voteCount++;
        if (voteCount > majority) {
            store.setTerm(store.getTerm() + 1);
            LeaderElected msg = new LeaderElected(store.getLogId(), store.getTerm(), brokerId);
            return ByteBuffer.wrap(msg.serialize());
        }

        return null;
    }

    public ByteBuffer handleLeaderElected(LeaderElected msg) {
        // Re-assign the controllerAddr.
        // TODO: Implement a form of handshaking between brokers to
        //       ensure that each peer is alive.
        return ByteBuffer.wrap(new byte[0]);
    }

    private void queueWrite(SocketChannel channel, ByteBuffer data) {
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(data.duplicate());

        SelectionKey key = channel.keyFor(serverSelector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            serverSelector.wakeup();
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Queue<ByteBuffer> queue = pendingWrites.get(channel);

        if (queue == null || queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }

        ByteBuffer buffer = queue.peek();
        while (buffer != null) {
            int bytesWritten = channel.write(buffer);

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

    public void sendVoteRequest(VoteRequest request) {
        int count = 0;
        for (SelectionKey key : serverSelector.keys()) {
            count++;
            SocketChannel channel = (SocketChannel) key.channel();
            queueWrite(channel, ByteBuffer.wrap(request.serialize()));
        }

        majority = count / 2 + 1;
    }

    private void connectToController() throws IOException {
//        Selector selector = Selector.open();
//        SocketChannel channel = SocketChannel.open(controllerAddr);
        clientSelector = Selector.open();
        clientSocketChannel = SocketChannel.open(controllerAddr);

//        channel.configureBlocking(false);
//        channel.register(selector, SelectionKey.OP_CONNECT);
        clientSocketChannel.configureBlocking(false);
        clientSocketChannel.register(clientSelector, SelectionKey.OP_CONNECT);

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    try {
                        sendHeartbeat();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, 0, 1000, TimeUnit.MILLISECONDS
        );

        while (running) {
            int readyCount = clientSelector.select();
            if (readyCount == 0) continue;

            Iterator<SelectionKey> iterator = clientSelector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();

                if (!key.isValid()) continue;

                try {
                    if (key.isConnectable()) handleClientConnect(key);
                    else if (key.isReadable()) handleClientRead(key);
                    else if (key.isWritable()) handleClientWrite(key);
                } catch (Exception e) {
                    e.printStackTrace();
                    cleanup(key);
                }
            }
        }
    }

    /**
     * Periodically ends heartbeat messages to a controller
     */
    private void sendHeartbeat() throws InterruptedException {
        Random random = new Random();
        int duration = random.nextInt(2000);
        Thread.sleep(duration);

        PingRequest request = new PingRequest();
        byte[] requestBytes = request.serialize();
        queueClientWrite(clientSocketChannel, ByteBuffer.wrap(requestBytes));
    }

    private void queueClientWrite(SocketChannel channel, ByteBuffer data) {
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(data.duplicate());

        SelectionKey key = channel.keyFor(clientSelector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            clientSelector.wakeup();
        }
    }

    private void handleClientConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        channel.configureBlocking(false);
        channel.socket().setKeepAlive(true);
        channel.socket().setTcpNoDelay(true);

        key.attach(new ClientSession(channel));
    }

    private void handleClientRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();

        ByteBuffer readBuffer = session.getReadBuffer();
        int readCount = channel.read(readBuffer);
        readBuffer.clear();

        if (readCount == -1) {
            // Start the election process.
            long nextTerm = store.getTerm() + 1;
            long logId = store.getLogId();
            VoteRequest request = new VoteRequest(nextTerm, logId, brokerId);
            sendVoteRequest(request);
            cleanup(key); // Close the connection to the dead controller
        }
    }

    private void handleClientWrite(SelectionKey key) throws IOException {
    }

    public void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;
    }

    public boolean isRunning() {
        return running;
    }

    private static class ClientSession {
        private static final int BUFFER_SIZE = 8192;
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
