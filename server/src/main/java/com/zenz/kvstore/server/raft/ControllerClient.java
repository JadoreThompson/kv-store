package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handler.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class ControllerClient {

    private final InetSocketAddress remoteAddress;
    private final Manager manager;
    private boolean shouldSendHeartbeat;
    private boolean initialised;
    private final List<RaftLogHandler.Log> logs = new ArrayList<>();
    private final KVStore kvStore;
    private final RaftLogHandler logHandler;

    private Selector selector;
    private SocketChannel socketChannel;
    private final Queue<ByteBuffer> pendingWrites = new LinkedList<>();
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private ClientStatus status = ClientStatus.DISCONNECTED;
    private boolean isRunning;

    // Debug
    private final String DEBUG_PREFIX;

    public ControllerClient(InetSocketAddress remoteAddress, Manager manager) {
        this.remoteAddress = remoteAddress;
        this.manager = manager;
        this.kvStore = manager.getKVStore();
        this.logHandler = (RaftLogHandler) this.kvStore.getLogHandler();
        this.DEBUG_PREFIX =
                String.format(
                        "[%s][BrokerClient %s:%s]",
                        this.manager.getNodeConfig().name(),
                        remoteAddress.getHostString(),
                        remoteAddress.getPort());
    }

    public void start() throws IOException {
        if (this.status != ClientStatus.DISCONNECTED) {
            return;
        }

        this.selector = Selector.open();
        openSocketChannel();
        this.isRunning = true;

        while (this.isRunning) {
            final int readyCount = selector.select(100);

            if (this.shouldSendHeartbeat && this.status == ClientStatus.CONNECTED) {
                queueWrite(ByteBuffer.wrap(new HeartbeatRequest().serialize()));
                this.shouldSendHeartbeat = false;
            }

            if (readyCount == 0) {
                continue;
            }

            final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                final SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) {
                    continue;
                }

                try {
                    if (key.isConnectable()) {
                        if (this.status == ClientStatus.CONNECTED && ((SocketChannel) key.channel()).finishConnect()) {
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
                            handleConnect();
                        } else {
                            retryConnection(key);
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
                if (!this.pendingWrites.isEmpty()) {
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

        this.isRunning = false;
        this.socketChannel.close();
        this.selector.close();
        this.pendingWrites.clear();
        this.readBuffer.clear();
        this.readBuffer.rewind();
    }

    private void queueWrite(ByteBuffer buffer) {
        this.pendingWrites.offer(buffer.duplicate());

        SelectionKey key = this.socketChannel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    /**
     * Loads all logs from log file and sends a request for entries to the controller with
     * the latest log id and term
     *
     * @throws IOException
     */
    private void handleConnect() throws IOException {
        if (this.initialised) {
            return;
        }

        loadLogs();

        final RaftLogHandler logHandler = (RaftLogHandler) this.manager.getKVStore().getLogHandler();
        long curLogId = logHandler.getLogId();
        long curTerm = logHandler.getTerm();

        if (!this.logs.isEmpty()) {
            RaftLogHandler.Log lastLog = this.logs.getLast();
            curLogId = lastLog.id();
            curTerm = lastLog.term();
        }

        queueWrite(ByteBuffer.wrap(new RequestEntry(curLogId, curTerm).serialize()));
        this.initialised = true;
    }

    private void loadLogs() throws IOException {
        // Adding the last command processed within the snapshot.
        File[] files = this.kvStore.getSnapshotter().getDir().toFile().listFiles();
        if (files.length > 0 && this.logHandler.getLog() != null) {
            this.logs.add(this.logHandler.getLog());
        }

        Path path = this.logHandler.getLogger().getPath();
        this.logs.addAll(RaftLogHandler.deserialize(path));
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        while (true) {
            if (readBuffer.position() >= readBuffer.capacity() - 1) {
                final int oldCap = readBuffer.capacity();
                final int newCap = (int) (oldCap * 1.6);

                final ByteBuffer newReadBuffer = ByteBuffer.allocate(newCap);
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
        boolean processed = processData();
        readBuffer.flip();

        if (processed) {
            readBuffer.clear();
        } else {
            readBuffer.position(prevPosition);
        }
    }

    private boolean processData() throws IOException {
        final String debugPrefix = DEBUG_PREFIX + "[processData] ";
        Message message;

        try {
            message = Message.deserialize(readBuffer);
        } catch (IllegalArgumentException e) {
            return true;
        }

        if (message == null) return false;

        ByteBuffer responseBuffer = null;
        if (message.type().equals(MessageType.APPEND_ENTRY)) {
            responseBuffer = handleAppendEntry((AppendEntry) message);
        } else if (message.type().equals(MessageType.INSTALL_SNAPSHOT)) {
            responseBuffer = handleInstallSnapshot((InstallSnapshot) message);
        } else if (message.type().equals(MessageType.HEARTBEAT_RESPONSE)) {
            this.manager.setLastHeartbeatTs(System.currentTimeMillis());
        }

        if (responseBuffer != null) {
            queueWrite(responseBuffer);
        }

        return true;
    }

    public ByteBuffer handleAppendEntry(AppendEntry message) throws IOException {
        final long curLogId = this.logHandler.getLogId();
        final long curTerm = this.logHandler.getTerm();

        // Nothing to process. We're up to date.
        if (curLogId == message.id() && curTerm == message.term()) return null;

        // Processing all commands.
        List<RaftLogHandler.Log> commands = message.entries();
        for (RaftLogHandler.Log command : commands) {
            if (command.term() != this.logHandler.getTerm()) {
                this.logHandler.setTerm(command.term());
            }

            if (command.command().type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command.command();
                this.kvStore.put(comm.key(), comm.value());
            } else if (command.command().type().equals(CommandType.DELETE)) {
                DeleteCommand comm = (DeleteCommand) command.command();
                this.kvStore.delete(comm.key());
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
    private ByteBuffer handleInstallSnapshot(InstallSnapshot message) throws IOException {
        final String M = "[handleInstallSnapshot] ";

        final long lastLogId = message.logId();
        final long lastTerm = message.term();
        final byte[] snapshot = message.snapshot();

        final KVMapSnapshotter snapshotter = this.kvStore.getSnapshotter();
        final Path snapshotDir = snapshotter.getDir();
        final Path snapshotPath = snapshotDir.resolve(lastLogId + "_" + lastTerm + ".snapshot");
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

        final Path logFpath = this.logHandler.getLogger().getPath();
        Files.delete(logFpath);
        Files.createFile(logFpath);
        this.logHandler.setLogger(new WALogger(logFpath));
        this.manager.setCurrentTerm(this.logHandler.getTerm());

        return ByteBuffer.wrap(new AppendEntryResponse(lastLogId, lastTerm, true).serialize());
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

    public Manager getNode() {
        return this.manager;
    }

    public void sendHeartbeat() {
        this.shouldSendHeartbeat = true;
    }
}
