package com.zenz.kvstore.raft;

import com.zenz.kvstore.KVRaftStore;
import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.messages.LeaderElected;
import com.zenz.kvstore.messages.Message;
import com.zenz.kvstore.messages.VoteRequest;
import com.zenz.kvstore.messages.VoteResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class KVRaftBroker {
    private final long brokerId;
    private final KVRaftStore store;
    private final String host;
    private final int port;
    private Server server;
    private Thread serverThread;
    private ArrayList<InetSocketAddress> peerAddrs = new ArrayList<>();
    private ArrayList<Client> peerClients = new ArrayList<>();
    private ArrayList<Thread> peerThreads = new ArrayList<>();
    private InetSocketAddress controllerAddr;
    private Client controllerClient;
    private Thread controllerThread;
    private KVRaft raft;
    private int voteCount = -1;
    private int majorityVoteCount = -1;

    public KVRaftBroker(
            KVRaft raft,
            KVRaftStore store,
            long brokerId,
            String host,
            int port,
            InetSocketAddress controllerAddr
    ) {
        this.raft = raft;
        this.store = store;
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.controllerAddr = controllerAddr;
    }

    public void addPeer(InetSocketAddress addr) {
        if (server != null && server.isRunning()) return;
        peerAddrs.add(addr);
        return;
    }

    public void removePeer(InetSocketAddress addr) {
        if (server != null && server.isRunning()) return;
        peerAddrs.remove(addr);
        return;
    }

    /**
     * Creates a socket server to receive connections
     * from peers. Also creates sockets to connect to
     * the controller as well as peers.
     */
    public void start() throws InterruptedException, IOException {
        launchServer();

        while (serverThread == null || !serverThread.isAlive()) {
            System.out.println("Waiting for server to start");
            Thread.sleep(500);
        }

        System.out.println("Server started");
        launchClients();
    }

    public void stop() {
    }

    /**
     * Launches a socket server which peer brokers will
     * connect to.
     */
    private void launchServer() {
        if (server != null && server.isRunning()) return;

        server = new Server(host, port);
        serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                // Ignore
            }
        });
        serverThread.start();
    }

    /**
     * Launches connections to all peer brokers and the controller
     */
    private void launchClients() throws IOException {
        if (server == null || !server.isRunning()) throw new RuntimeException("Server not running");

        // Launching connections to peer brokers
        for (InetSocketAddress addr : peerAddrs) {
            Client client = new Client(addr.getHostName(), addr.getPort());
            peerClients.add(client);
            Thread th = new Thread(() -> {
                try {
                    client.start();
                } catch (Exception e) {
                }
            });
            peerThreads.add(th);
            th.start();
        }

        // Launching connection to controller
        launchControllerClient();
    }

    private void launchControllerClient() throws IOException {
        Client controllerClient = new Client(controllerAddr.getHostName(), controllerAddr.getPort());
        controllerThread = new Thread(() -> {
            try {
                controllerClient.start();
            } catch (Exception e) {
            }
        });
        controllerThread.start();
    }

    public ByteBuffer handleVoteResponse(VoteResponse response) {
        if (voteCount == -1) return ByteBuffer.wrap("ERROR Not a candidate".getBytes(StandardCharsets.UTF_8));
        if (!response.voteGranted()) return ByteBuffer.wrap(new byte[0]);

        voteCount++;
        if (voteCount > majorityVoteCount) {
            store.setTerm(store.getTerm() + 1);
            LeaderElected msg = new LeaderElected(store.getLogId(), store.getTerm(), brokerId);
            raft.convert(this);
            return ByteBuffer.wrap(msg.serialize());
        }

        return null;
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

    /**
     * Handles the leader elected message. Removes the address from the peer
     * list. Re-assigns the controller address adn re-establishes a connection
     * to the new leader and re instantiates the controller thread.
     *
     * @param message
     * @param addr
     */
    private void handleLeaderElected(LeaderElected message, InetSocketAddress addr) throws IOException {
        // Removing peer connection
        Iterator<Thread> it = peerThreads.iterator();
        while (it.hasNext()) {
            Thread th = it.next();
            if (th.getName().equals(addr.getHostString())) {
                th.interrupt();
                it.remove();
            }
        }

        // Re-creating controller connection
        controllerClient.stop();
        controllerThread.interrupt();
        controllerAddr = addr;

        launchControllerClient();
        voteCount = -1;
        majorityVoteCount = -1;
    }

    public KVRaftStore getStore() {
        return store;
    }

    public Selector getSelector() {
        return server.selector;
    }

    public ServerSocketChannel getServerSocketChannel() {
        return server.serverSocketChannel;
    }

    private class Server {
        private final String host;
        private final int port;
        private Selector selector;
        private ServerSocketChannel serverSocketChannel;
        private HashMap<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
        private boolean running = false;

        public Server(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public void start() throws IOException {
            if (running) return;

            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(host, port));
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            running = true;

            while (running) {
                int readyCount = selector.select();
                if (readyCount == 0) continue;

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
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

        public void stop() throws IOException {
            if (!running) return;
        }

        public void handleAccept(SelectionKey key) throws IOException {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel = server.accept();

            if (channel == null) return;

            channel.configureBlocking(false);
            channel.socket().setTcpNoDelay(true);
            channel.socket().setKeepAlive(true);

            // Register for READ events with session attachment
            SelectionKey clientKey = channel.register(selector, SelectionKey.OP_READ);
            clientKey.attach(new ClientSession(channel));
        }

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

                Message msg = Message.deserialize(buffer);
                ByteBuffer responseBuffer = null;
                if (msg.type().equals(MessageType.VOTE_REQUEST)) {
                    responseBuffer = handleVoteRequest((VoteRequest) msg);
                } else if (msg.type().equals(MessageType.VOTE_RESPONSE)) {
                    responseBuffer = handleVoteResponse((VoteResponse) msg);
                } else if (msg.type().equals(MessageType.LEADER_ELECTED)) {
                    handleLeaderElected((LeaderElected) msg, (InetSocketAddress) channel.getRemoteAddress());
                } else {
                    responseBuffer = ByteBuffer.wrap(("ERROR Invalid message type " + msg.type().name()).getBytes(StandardCharsets.UTF_8));
                }

                if (responseBuffer != null) queueWrite(channel, responseBuffer);

                buffer.clear();
                buffer.flip();
            } catch (IllegalArgumentException e) {
                // Ignore
                buffer.position(position);
                buffer.flip();
            }
        }

//        public ByteBuffer handleVoteRequest(VoteRequest request) {
//            long curTerm = store.getTerm();
//            long curLogId = store.getLogId();
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
//            return ByteBuffer.wrap(response.serialize());
//        }

//        public ByteBuffer handleVoteResponse(VoteResponse response) {
//            if (voteCount == -1) return ByteBuffer.wrap("ERROR Not a candidate".getBytes(StandardCharsets.UTF_8));
//            if (!response.voteGranted()) return ByteBuffer.wrap(new byte[0]);
//
//            voteCount++;
//            if (voteCount > majorityVoteCount) {
//                store.setTerm(store.getTerm() + 1);
//                LeaderElected msg = new LeaderElected(store.getLogId(), store.getTerm(), brokerId);
//                raft.convert(this);
//                return ByteBuffer.wrap(msg.serialize());
//            }
//
//            return null;
//        }

        private void queueWrite(SocketChannel channel, ByteBuffer data) {
            Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
            queue.offer(data.duplicate());

            SelectionKey key = channel.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        }

        public void handleWrite(SelectionKey key) {
        }

        public void cleanup(SelectionKey key) {
            if (!key.isValid()) return;
        }

        public boolean isRunning() {
            return running;
        }

        private static class ClientSession {
            private static final int BUFFER_SIZE = 8192;
            private final SocketChannel channel;
            private final ByteBuffer readBuffer;

            ClientSession(SocketChannel client) {
                this.channel = client;
                this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            }

            SocketChannel getChannel() {
                return channel;
            }

            ByteBuffer getReadBuffer() {
                return readBuffer;
            }
        }
    }

    private class Client {
        private final String remoteHost;
        private final int remotePort;
        private Selector selector;
        private SocketChannel channel;
        private boolean running = false;

        public Client(String remoteHost, int remotePort) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
        }

        public void start() throws IOException {
            if (running) return;

            selector = Selector.open();
            channel = SocketChannel.open(new InetSocketAddress(remoteHost, remotePort));
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_CONNECT);

            running = true;
        }

        public void stop() {
            if (!running) return;
        }

        public boolean isRunning() {
            return running;
        }
    }
}
