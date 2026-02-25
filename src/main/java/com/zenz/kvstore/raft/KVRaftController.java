package com.zenz.kvstore.raft;

import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.KVRaftStore;
import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.messages.BrokerLogStateRequest;
import com.zenz.kvstore.messages.ControllerLogStateResponse;
import com.zenz.kvstore.messages.Message;
import com.zenz.kvstore.messages.PingResponse;
import com.zenz.kvstore.operations.Operation;
import com.zenz.kvstore.operations.RaftOperation;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class KVRaftController {
    private final KVRaftStore store;
    private final String host;
    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private HashMap<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
    private HashMap<Long, ClientSession> sessions = new HashMap<>();
    private Deque<RaftOperation> commands = new ArrayDeque<>();
    private int commandsPerFlush;
    private boolean running = false;

    public KVRaftController(KVRaftStore store, String host, int port) {
        this.store = store;
        this.host = host;
        this.port = port;
        commandsPerFlush = store.getLogsPerSnapshot();
    }

    public KVRaftController(KVRaftBroker broker) {
        store = broker.getStore();
        selector = broker.getSelector();
        serverSocketChannel = broker.getServerSocketChannel();
        host = serverSocketChannel.socket().getInetAddress().getHostAddress();
        port = serverSocketChannel.socket().getLocalPort();
        commandsPerFlush = store.getLogsPerSnapshot();
        running = false;
    }

    public void start() throws IOException {
        if (running) return;

        if (selector == null) {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            serverSocketChannel.socket().bind(new InetSocketAddress(host, port));
        }

        running = true;

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

        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);
    }

    public void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();

        ByteBuffer buffer = session.getReadBuffer();
        int bytesRead = channel.read(buffer);

        // Client disconnected
        if (bytesRead == -1) {
            cleanup(key);
            return;
        }

        if (bytesRead == 0) return;

        buffer.flip();
        processData(session, buffer);
        buffer.compact();
    }

    private void processData(ClientSession session, ByteBuffer buffer) throws IOException {
        int currentPosition = buffer.position();
        buffer.rewind();

        Message message;

        try {
            message = Message.deserialize(buffer);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            // Resetting position to original position as we assume there weren't
            // enough bytes within the buffer to form a coherent message
            buffer.position(currentPosition);
            return;
        }

        // Clearing current message from buffer
        buffer.clear();
        SocketChannel channel = session.getChannel();
        ByteBuffer respBuffer;

        if (message.type().equals(MessageType.PING_REQUEST)) {
//            respBuffer = ByteBuffer.wrap(new PingResponse().serialize());
            respBuffer = ByteBuffer.wrap(new PingResponse().serialize());
        } else if (message.type().equals(MessageType.BROKER_LOG_STATE_REQUEST)) {
            respBuffer = handleBrokerLogStateRequest((BrokerLogStateRequest) message, channel);
        } else {
            respBuffer = ByteBuffer.wrap("ERROR: Unknown operation type".getBytes(StandardCharsets.UTF_8));
        }

        queueWrite(channel, respBuffer);
    }

    private void queueWrite(SocketChannel channel, ByteBuffer data) {
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(data.duplicate());

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private ByteBuffer handleBrokerLogStateRequest(BrokerLogStateRequest message, SocketChannel channel) throws IOException {
        SelectionKey key = channel.keyFor(selector);

        ClientSession session = sessions.get(message.brokerId());
        if (session == null) {
            session = new ClientSession(channel, message.brokerId());
            key.attach(session);
            sessions.put(session.getBrokerId(), session);
        }

        if (message.id() == store.getLogId()) return null;

        if (message.id() == store.getLogId() - 1) {
            RaftOperation existing = commands.getLast();
            if (existing != null) {
                return ByteBuffer.wrap(new ControllerLogStateResponse(ControllerLogStateResponse.Type.LOG, null, existing).serialize());
            }
        }

        RaftOperation existing = commands.getFirst();
        if (existing == null) {
            KVMapSnapshotter snapshotter = store.getSnapshotter();
            File[] files = snapshotter.getFolderPath().toFile().listFiles();
            File snapshotFile = files[files.length - 1];
            return ByteBuffer.wrap(new ControllerLogStateResponse(ControllerLogStateResponse.Type.SNAPSHOT, Files.readAllBytes(snapshotFile.toPath()), null).serialize());
        }

        RaftOperation front = commands.getFirst();

        session.setLogState(new BrokerLogState(message.id(), message.term(), message.operation()));

        return ByteBuffer.wrap("OK".getBytes(StandardCharsets.UTF_8));
    }

    public void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Queue<ByteBuffer> queue = pendingWrites.get(channel);

        if (queue == null || queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }

        ByteBuffer buffer = queue.peek(); // Retrieve head
        while (buffer != null) {
            int bytesWritten = channel.write(buffer);

            if (bytesWritten == 0) {
                break;  // Send buffer full
            }

            if (!buffer.hasRemaining()) {
                // Retrieve and remove head, advancing head to the next element
                queue.poll();
                buffer = queue.peek();
            }
        }

        if (queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    public void handleCommand(RaftOperation operation) {
        if (commands.size() >= commandsPerFlush) {
            commands.pollFirst();
            commands.addLast(operation);
        }

        Collection<ClientSession> s = sessions.values();
        ArrayList<SocketChannel> toSend = new ArrayList<>();
        int count = 0;

        for (ClientSession session : s) {
            if (session.logState == null || session.logState.id + 1 < operation.id()) continue;
            count++;
            toSend.add(session.getChannel());
        }

        if (count > 0) {
            for (SocketChannel channel : toSend) {
                queueWrite(channel, ByteBuffer.wrap(operation.serialize()));
            }
        }
    }

    public void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;
        SelectableChannel channel = key.channel();
        pendingWrites.remove(channel);

        key.cancel();

        try {
            channel.close();
        } catch (IOException e) {
        }
    }

    private static class ClientSession {
        private static final int BUFFER_SIZE = 8192;

        private final long brokerId;
        private final SocketChannel channel;
        private final ByteBuffer readBuffer;
        private final BrokerLogState logState;

        ClientSession(SocketChannel client, long brokerId) {
            this.channel = client;
            this.brokerId = brokerId;
            this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            this.logState = null;
        }

        long getBrokerId() {
            return brokerId;
        }

        SocketChannel getChannel() {
            return channel;
        }

        ByteBuffer getReadBuffer() {
            return readBuffer;
        }

        BrokerLogState getLogState() {
            return logState;
        }

        void setLogState(BrokerLogState logState) {
        }
    }

    private static class BrokerLogState {
        public long id;
        public long term;
        public Operation operation;

        public BrokerLogState(long id, long term, Operation operation) {
            this.id = id;
            this.term = term;
            this.operation = operation;
        }
    }
}
