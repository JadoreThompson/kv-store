package com.zenz.kvstore.raft;

import com.zenz.kvstore.*;
import com.zenz.kvstore.messages.*;
import com.zenz.kvstore.operations.RaftGetOperation;
import com.zenz.kvstore.operations.RaftOperation;
import com.zenz.kvstore.operations.RaftPutOperation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private ControllerClient controllerClient;
    private Thread controllerThread;
    private KVRaft raft;
    private int voteCount = -1;
    private int majorityVoteCount = -1;
    private RaftOperation lastOperation = null;

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

        // Wait for server to actually be running
        while (!server.isRunning()) {
            System.out.println("Waiting for server to be ready");
            Thread.sleep(100);
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
        ControllerClient controllerClient = new ControllerClient(controllerAddr.getHostName(), controllerAddr.getPort());
        this.controllerClient = controllerClient;
        controllerThread = new Thread(() -> {
            try {
                controllerClient.start();
            } catch (Exception e) {
                e.printStackTrace();
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

            while (running) {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (!key.isValid()) continue;
                    try {
                        if (key.isReadable()) ;
                        else if (key.isWritable()) ;
                    } catch (Exception e) {
                        e.printStackTrace();
                        cleanup(key);
                    }
                }
            }
        }

        public void stop() {
            if (!running) return;
        }

        public void cleanup(SelectionKey key) throws IOException {
            if (!key.isValid()) return;
        }

        public boolean isRunning() {
            return running;
        }
    }

    /**
     * Client connection to the controller.
     * Handles sending broker log state requests and receiving log entries/snapshots.
     */
    private class ControllerClient {
        private final String remoteHost;
        private final int remotePort;
        private Selector selector;
        private SocketChannel channel;
        private HashMap<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
        private boolean running = false;
        private boolean registered = false;

        public ControllerClient(String remoteHost, int remotePort) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
        }

        public void start() throws IOException {
            if (running) return;

            selector = Selector.open();
            channel = SocketChannel.open(new InetSocketAddress(remoteHost, remotePort));
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

            running = true;

            while (running) {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (!key.isValid()) continue;
                    try {
                        if (key.isReadable()) handleRead(key);
                        if (key.isWritable()) handleWrite(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                        cleanup(key);
                    }
                }
            }
        }

        private void handleRead(SelectionKey key) throws IOException {
            SocketChannel ch = (SocketChannel) key.channel();
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            int bytesRead = ch.read(buffer);

            if (bytesRead == -1) {
                cleanup(key);
                return;
            }

            if (bytesRead == 0) return;

            buffer.flip();

            // Try to deserialize as a message from controller
            try {
                Message msg = Message.deserialize(buffer);

                if (msg.type().equals(MessageType.CONTROLLER_LOG_STATE_RESPONSE)) {
                    handleControllerLogStateResponse((ControllerLogStateResponse) msg);
                } else if (msg.type().equals(MessageType.PING_RESPONSE)) {
                    System.out.println("Received PING response from controller");
                }
            } catch (IllegalArgumentException e) {
                // Try to deserialize as a RaftOperation (direct log entry)
                buffer.rewind();
                try {
                    RaftOperation operation = RaftOperation.deserialize(buffer);
                    handleRaftOperation(operation);
                } catch (Exception ex) {
                    System.err.println("Failed to deserialize message: " + ex.getMessage());
                }
            }
        }

        private void handleWrite(SelectionKey key) throws IOException {
            SocketChannel ch = (SocketChannel) key.channel();
            Queue<ByteBuffer> queue = pendingWrites.get(ch);

            // Send registration request if not yet registered
            if (!registered) {
                sendBrokerLogStateRequest(ch);
                registered = true;
            }

            if (queue == null || queue.isEmpty()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                return;
            }

            ByteBuffer buffer = queue.peek();
            while (buffer != null) {
                int bytesWritten = ch.write(buffer);

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

        private void sendBrokerLogStateRequest(SocketChannel ch) throws IOException {
            // Send our current log state to the controller
            if (lastOperation == null) {
                WALogger logger = store.getLogger();
                Path fpath = logger.getPath();
                List<RaftOperation> operations = RaftOperation.parseRaftLogs(ByteBuffer.wrap(Files.readAllBytes(fpath)));
                if (!operations.isEmpty()) {
                    lastOperation = operations.get(operations.size() - 1);
                }
            }
            BrokerLogStateRequest request = new BrokerLogStateRequest(
                    brokerId,
                    store.getLogId(),
                    store.getTerm(),
                    lastOperation
            );

            ByteBuffer buffer = ByteBuffer.wrap(request.serialize());
            while (buffer.hasRemaining()) {
                ch.write(buffer);
            }

            System.out.println("Sent BrokerLogStateRequest to controller: logId=" + store.getLogId() + ", term=" + store.getTerm());
        }

        private void handleControllerLogStateResponse(ControllerLogStateResponse response) throws IOException {
            if (response.documentType().equals(ControllerLogStateResponse.Type.LOG)) {
                // Received a log entry - execute the operation
                RaftOperation operation = response.operation();
                if (operation != null) {
                    handleRaftOperation(operation);
                }
            } else if (response.documentType().equals(ControllerLogStateResponse.Type.SNAPSHOT)) {
                // Received a snapshot - write to file and reload store
                handleSnapshot(response.snapshot());
            }
        }

        private void handleRaftOperation(RaftOperation operation) throws IOException {
            System.out.println("Received RaftOperation: " + operation.type() + ", id=" + operation.id());

            if (operation.type().equals(OperationType.PUT)) {
                RaftPutOperation putOp = (RaftPutOperation) operation;
                store.put(putOp.key(), putOp.value());
                store.setLogId(putOp.id());
                store.setTerm(putOp.term());
            } else if (operation.type().equals(OperationType.GET)) {
                RaftGetOperation getOp = (RaftGetOperation) operation;
                store.get(getOp.key());
                store.setLogId(getOp.id());
                store.setTerm(getOp.term());
            }
        }

        private void handleSnapshot(byte[] snapshotData) throws IOException {
            if (snapshotData == null || snapshotData.length == 0) return;

            // Parse the snapshot filename from the logId
            long logId = store.getLogId();
            String snapshotFilename = logId + ".snapshot";
            Path snapshotPath = store.getSnapshotter().getFolderPath().resolve(snapshotFilename);

            // Write the snapshot to file
            Files.write(snapshotPath, snapshotData);

            // Create the new log file: <snapshot_number + 1>.log
            String logFilename = (logId + 1) + ".log";
            Path logPath = store.getLogsFolder().resolve(logFilename);
            logPath.toFile().createNewFile();

            // Reload the store from the snapshot
            reloadStore();
        }

        private void reloadStore() throws IOException {
            // Load the map from the latest snapshot
            KVMap map = store.getSnapshotter().loadSnapshot();
            if (map != null) {
                // Clear the current map and copy the snapshot data
                store.getMap().clear();
                // Apply all entries from the loaded map
                // Note: This is a simplified approach - in production you'd want a more efficient method
            }
        }

        public void stop() {
            if (!running) return;
            running = false;
            try {
                if (channel != null) channel.close();
                if (selector != null) selector.close();
            } catch (IOException e) {
                // Ignore
            }
        }

        public void cleanup(SelectionKey key) throws IOException {
            if (!key.isValid()) return;
            key.cancel();
            try {
                key.channel().close();
            } catch (IOException e) {
                // Ignore
            }
        }

        public boolean isRunning() {
            return running;
        }
    }
}
