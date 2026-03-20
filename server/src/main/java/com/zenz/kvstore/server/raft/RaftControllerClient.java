package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.server.*;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.messages.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RaftControllerClient {
    private final InetSocketAddress controllerAddress;
    private Selector selector;
    private SocketChannel socketChannel;
    private final Queue<ByteBuffer> pendingWrites = new LinkedList<>();
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private boolean isRunning;

    private final KVStore store;
    private final RaftLogHandler logHandler;
    private ArrayList<RaftLogHandler.Log> logs = new ArrayList<>();
    private CompletableFuture<Boolean> onDisconnect;
    private final Random random = new Random();
    private final RaftManager manager;
    private long lastHeartBeatResponse;
    private final String DEBUG_PREFIX;
    private final CompletableFuture<Boolean> joinFut = new CompletableFuture<>();

    public RaftControllerClient(
            String host,
            int port,
            KVStore store,
            RaftManager manager
    ) {
        controllerAddress = new InetSocketAddress(host, port);
        this.store = store;
        this.logHandler = (RaftLogHandler) store.getLogHandler();
        this.manager = manager;
        DEBUG_PREFIX = String.format("[nodeId=%s RaftControllerClient]", manager.getConfig().id());
    }

    public void start() throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[start]";
        if (isRunning) return;

        selector = Selector.open();
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(controllerAddress);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);

        isRunning = true;

        int timeout = random.nextInt(1000);
        long lastHeartbeatRequest = 0;
        boolean connected = false;

        while (isRunning) {
            int readyCount = selector.select(timeout);

            // Handling heartbeats
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastHeartbeatRequest > timeout) {
                if (lastHeartBeatResponse == 0) {
                    lastHeartBeatResponse = currentTime;
                }

                if (currentTime - lastHeartBeatResponse > 2000) {
                    // Assuming controller has disconnected.
                    initiateElection();
                    stop();
                    break;
                }

                sendHeartBeat();
                lastHeartbeatRequest = currentTime;
                timeout = random.nextInt(1000);
            }

            if (readyCount == 0) continue;

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) continue;

                try {
                    if (key.isConnectable()) {
                        if (!connected) {
                            if (socketChannel.isConnectionPending()) {

                                try {
                                    socketChannel.finishConnect();
                                    connected = true;
                                    // Update interest ops from OP_CONNECT to OP_READ, preserving OP_WRITE if there are pending writes
                                    int newOps = SelectionKey.OP_READ;
                                    if (!pendingWrites.isEmpty()) {
                                        newOps |= SelectionKey.OP_WRITE;
                                    }
                                    key.interestOps(newOps);
                                } catch (IOException e) {
                                    // Close the old channel and retry
                                    socketChannel.close();
                                    socketChannel = SocketChannel.open();
                                    socketChannel.configureBlocking(false);
                                    socketChannel.connect(controllerAddress);
                                    socketChannel.register(selector, SelectionKey.OP_CONNECT);
                                }
                            } else {
                                socketChannel.connect(controllerAddress);
                            }
                        } else if (((SocketChannel) key.channel()).finishConnect()) {
                            // Connection established, remove OP_CONNECT interest
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
                            // Now safe to call handleConnect
                            handleConnect(key);
                        }


                    } else if (key.isReadable()) handleRead(key);
                    else if (key.isWritable()) handleWrite(key);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        joinFut.complete(true);
    }

    public void stop() throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[stop] ";
        if (!isRunning) {
            return;
        }

        isRunning = false;
        socketChannel.close();
        selector.close();
        pendingWrites.clear();
        readBuffer.clear();
        logs.clear();
    }

    private void join() {
        try {
            joinFut.get();
        } catch (Exception e) {
        }
    }

    private void handleConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        loadLogs();
        long curLogId = logHandler.getLogId();
        long curTerm = logHandler.getTerm();
        if (!logs.isEmpty()) {
            RaftLogHandler.Log lastLog = null;
            for (RaftLogHandler.Log entry : logs) {
                if (entry == null) continue;
                lastLog = entry;
            }

            curLogId = lastLog.id();
            curTerm = lastLog.term();
        }

        queueWrite(channel, ByteBuffer.wrap(new RequestEntry(curLogId, curTerm).serialize()));
    }

    private void loadLogs() throws IOException {
        // Adding the last command processed within the snapshot.
        File[] files = store.getSnapshotter().getDir().toFile().listFiles();
        if (files.length > 0) {
            if (logHandler.getLog() != null) logs.add(logHandler.getLog());
        }

        Path path = logHandler.getLogger().getPath();
        for (RaftLogHandler.Log logEntry : RaftLogHandler.deserialize(path)) {
            logs.add(logEntry);
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        final String M = DEBUG_PREFIX + "[handleRead] ";

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

            }

            int bytesRead = channel.read(readBuffer);
            totalBytesRead += bytesRead;

            if (bytesRead == -1) {
                cleanup(key);
                initiateElection();
                stop();
                return;
            }

            if (bytesRead == 0) {
                break;
            }

            if (readBuffer.position() < readBuffer.capacity() - 1) {
                break;
            }
        }


        if (totalBytesRead > 0) {
            readBuffer.flip();

            int position = readBuffer.position();

            boolean processed = processData(key);
            readBuffer.flip();


            if (processed) {
                readBuffer.compact();

            } else {
                readBuffer.position(position);
            }
        }
    }

    private void initiateElection() throws IOException {
        if (manager.getLastTerm() == ((RaftLogHandler) store.getLogHandler()).getTerm()) {
            manager.initiateElection();
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

        if (message == null) return false;

        ByteBuffer responseBuffer = null;
        if (message.type().equals(MessageType.APPEND_ENTRY)) {
            responseBuffer = handleAppendEntry((AppendEntry) message);
        } else if (message.type().equals(MessageType.INSTALL_SNAPSHOT)) {
            responseBuffer = handleAppendSnapshot((InstallSnapshot) message);
        } else if (message.type().equals(MessageType.HEARTBEAT_RESPONSE)) {
            lastHeartBeatResponse = System.currentTimeMillis();
        }

        if (responseBuffer != null) {
            queueWrite((SocketChannel) key.channel(), responseBuffer);
        }

        return true;
    }

    public ByteBuffer handleAppendEntry(AppendEntry message) throws IOException {
        long curLogId = logHandler.getLogId();
        long curTerm = logHandler.getTerm();

        // Nothing to process. We're up to date.
        if (curLogId == message.id() && curTerm == message.term()) return null;

        // Processing all commands.
        List<RaftLogHandler.Log> commands = message.entries();
        for (RaftLogHandler.Log command : commands) {
            if (command.term() != logHandler.getTerm()) {
                logHandler.setTerm(command.term());
            }

            if (command.command().type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command.command();
                store.put(comm.key(), comm.value());
            } else if (command.command().type().equals(CommandType.DELETE)) {
                DeleteCommand comm = (DeleteCommand) command.command();
                store.delete(comm.key());
            }
        }

        return ByteBuffer.wrap(new AppendEntryResponse(
                logHandler.getLogId(), logHandler.getTerm(), true
        ).serialize());
    }

    /**
     * Persist the snapshot to disk, clear the current log file as those logs are now
     * redundant. Return an acknowledgement containing the snapshot has been applied and send
     * the last log id and term known within the snapshot.
     *
     * @param message
     * @return
     * @throws IOException
     */
    private ByteBuffer handleAppendSnapshot(InstallSnapshot message) throws IOException {
        final String M = "[handleAppendSnapshot] ";


        long lastLogId = message.logId();
        long lastTerm = message.term();
        byte[] snapshot = message.snapshot();


        KVMapSnapshotter snapshotter = store.getSnapshotter();
        Path snapshotDir = snapshotter.getDir();


        Path snapshotPath = snapshotDir.resolve(lastLogId + "_" + lastTerm + ".snapshot");

        Files.createFile(snapshotPath);

        try (FileChannel fileChannel = FileChannel.open(snapshotPath, StandardOpenOption.WRITE)) {
            fileChannel.write(ByteBuffer.wrap(snapshot));
        }

        File[] files = snapshotDir.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.getName().equals(snapshotPath.getFileName().toString())) {
                    file.delete();
                }
            }
        }


        Path logFpath = store.getLogHandler().getLogger().getPath();

        Files.delete(logFpath);

        Files.createFile(logFpath);

        store.getLogHandler().setLogger(new WALogger(logFpath));


        ByteBuffer response = ByteBuffer.wrap(
                new AppendEntryResponse(lastLogId, lastTerm, true).serialize()
        );


        return response;
    }

    private void handleWrite(SelectionKey key) throws IOException {
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

    private void sendEntryRequest() throws IOException {
        if (logs == null) {
            Path path = logHandler.getLogger().getPath();
            logs = RaftLogHandler.deserialize(path);
        }

        long curLogId = logHandler.getLogId();
        long curTerm = logHandler.getTerm();

        queueWrite(socketChannel, ByteBuffer.wrap(new RequestEntry(curLogId, curTerm).serialize()));
    }

    private void sendHeartBeat() {
        queueWrite(socketChannel, ByteBuffer.wrap(new HeartbeatRequest().serialize()));
    }

    private void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;

        SelectableChannel channel = key.channel();
        channel.close();
        key.cancel();
        stop();
    }

    public boolean isRunning() {
        return isRunning;
    }

    public InetSocketAddress getControllerAddress() {
        return controllerAddress;
    }

    public KVStore getStore() {
        return store;
    }

    public RaftLogHandler getLogHandler() {
        return logHandler;
    }

    public CompletableFuture<Boolean> getOnDisconnect() {
        return onDisconnect;
    }

    public void setOnDisconnect(CompletableFuture<Boolean> onDisconnect) {
        this.onDisconnect = onDisconnect;
    }

    public RaftManager getManager() {
        return manager;
    }
}
