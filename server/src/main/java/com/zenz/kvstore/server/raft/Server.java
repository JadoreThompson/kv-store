package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.server.ClientSession;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.logging.handler.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Server {

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private boolean isRunning;

    // Server
    private final Manager manager;
    private final InetSocketAddress address;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();

    // Controller
    private final RaftLogHandler logHandler;
    private final KVMapSnapshotter snapshotter;
    private final ArrayList<RaftLogHandler.Log> logs = new ArrayList<>();
    private final ConcurrentLinkedQueue<CommandTask> commandTasks = new ConcurrentLinkedQueue<>();
    private CommandTask curCommandTask;

    private NodeRole prevRole;

    // Debug
    private final String DEBUG_PREFIX;

    public Server(InetSocketAddress address, Manager manager) {
        this.address = address;
        this.manager = manager;
        this.logHandler = (RaftLogHandler) manager.getKVStore().getLogHandler();
        this.snapshotter = manager.getKVStore().getSnapshotter();
        this.DEBUG_PREFIX = String.format("[%s][Server]", manager.getNodeConfig().name());
    }

    public void start() throws Exception {
        final String debugPrefix = this.DEBUG_PREFIX + "[start] ";
        if (this.isRunning) return;

        this.isRunning = true;

        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.socket().bind(this.address);
        this.serverChannel.register(this.selector, SelectionKey.OP_ACCEPT);

        while (this.isRunning) {
            int readyCount = this.selector.select(100);

            NodeRole curRole = this.manager.getRole();
            NodeRole prevRole = this.prevRole;
            this.prevRole = curRole;

            if (curRole == NodeRole.CONTROLLER) {
                if (prevRole != NodeRole.CONTROLLER) {
                    loadLogs();
                }
                checkPendingTask();
            }

            if (readyCount == 0) {
                continue;
            }

            Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                try {
                    if (!key.isValid()) {
                        cleanup(key);
                    } else if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    cleanup(key);
                }
            }
        }
    }

    public void stop() throws IOException {
        if (!this.isRunning) {
            return;
        }

        this.isRunning = false;

        if (this.selector != null) {
            this.selector.wakeup();
        }

        if (this.selector != null) {
            for (SelectionKey key : this.selector.keys()) {
                cleanup(key);
            }
            this.selector.close();
        }

        if (this.serverChannel != null) {
            this.serverChannel.close();
        }

        this.pendingWrites.clear();
    }

    public void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;

        SelectableChannel channel = key.channel();
        channel.close();
        key.cancel();
        pendingWrites.remove(channel);
    }

    private void queueWrite(SocketChannel channel, ByteBuffer buffer) {
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(buffer.duplicate());

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    public void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();

        if (channel == null) return;

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setKeepAlive(true);

        SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_READ);
        selectionKey.attach(new RaftClientSession(channel));
    }

    public void handleRead(SelectionKey key) throws IOException {
        final String debugPrefix = this.DEBUG_PREFIX + "[handleRead] ";
        RaftClientSession session = (RaftClientSession) key.attachment();
        ByteBuffer readBuffer = session.getReadBuffer();
        SocketChannel channel = (SocketChannel) key.channel();
        int totalBytesRead = 0;

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
            totalBytesRead += bytesRead;

            if (bytesRead == -1) {
                cleanup(key);
                return;
            }

            if (bytesRead == 0 || readBuffer.position() < readBuffer.capacity() - 1) {
                break;
            }
        }

        if (totalBytesRead == 0) {
            return;
        }

        session.setReadBuffer(readBuffer);
        final int prevPosition = readBuffer.position();
        readBuffer.flip();
        final boolean processed = processData(session, readBuffer);
        readBuffer.flip();

        if (processed) {
            readBuffer.clear();
        } else {
            // Assuming not enough bytes in message to parse successfully
            readBuffer.position(prevPosition);
        }
    }

    public void handleWrite(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        Queue<ByteBuffer> queue = pendingWrites.get(client);

        if (queue == null || queue.isEmpty()) {
            key.interestOps(SelectionKey.OP_READ);
            return;
        }

        ByteBuffer buffer = queue.peek();
        while (buffer != null) {
            final int bytesWritten = client.write(buffer);
            if (bytesWritten == 0) {
                break;  // Send buffer full
            }

            if (!buffer.hasRemaining()) {
                queue.poll();
                buffer = queue.peek();
            }
        }

        key.interestOps(SelectionKey.OP_READ);
    }

    private boolean processData(RaftClientSession session, ByteBuffer buffer) throws IOException {
        final String debugPrefix = this.DEBUG_PREFIX + "[processData] ";

        Message message;
        try {
            message = Message.deserialize(buffer);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            queueWrite(
                    session.getChannel(),
                    ByteBuffer.wrap(new ErrorMessage(RaftErrorType.INVALID_MESSAGE_TYPE, e.getMessage()).serialize())
            );
            return true;
        }

        // Not enough bytes to parse successfully
        if (message == null) {
            return false;
        }

        MessageType messageType = message.type();
        if (messageType == MessageType.REGISTER) {
            manager.handleRegisterMessage((RegisterMessage) message);
            return true;
        }

        ByteBuffer responseBuffer = null;
        final NodeRole role = this.manager.getRole();

        if (messageType == MessageType.REQUEST_VOTE) {
            responseBuffer = handleRequestVote((RequestVote) message);
        } else if (messageType == MessageType.LEADER_ELECTED) {
            this.manager.handleLeaderElected((LeaderElected) message);
        } else if (role == NodeRole.CONTROLLER) {
            if (messageType == MessageType.REQUEST_ENTRY) {
                responseBuffer = handleEntryRequest(session, (RequestEntry) message);
            } else if (messageType == MessageType.APPEND_ENTRY_RESPONSE) {
                responseBuffer = handleAppendEntryResponse(session, (AppendEntryResponse) message);
            } else if (messageType == MessageType.HEARTBEAT_REQUEST) {
                responseBuffer = ByteBuffer.wrap(new HeartbeatResponse().serialize());
            }
        }

        if (responseBuffer != null) {
            queueWrite(session.getChannel(), responseBuffer);
        }

        return true;
    }

    // === CONTROLLER METHODS ===

    private void loadLogs() throws IOException {
        // Adding the last command processed within the snapshot.
        File[] files = snapshotter.getDir().toFile().listFiles();
        if (files.length > 0) {
            logs.add(logHandler.getLog());
        }

        // Adding all logged commands
        Path path = logHandler.getLogger().getPath();
        logs.addAll(RaftLogHandler.deserialize(path));
    }

    public void handleCommand(Command command, CompletableFuture<Boolean> fut) {
        CommandTask task = new CommandTask(command, fut);
        commandTasks.add(task);
        selector.wakeup();
    }

    private void checkPendingTask() {
        if (curCommandTask == null && !commandTasks.isEmpty()) {
            CommandTask task = commandTasks.poll();
            if (task != null) {
                curCommandTask = task;
                // Trigger the private handleCommand to broadcast to followers
                handleCommandTask(curCommandTask);
            }
        }
    }

    private void handleCommandTask(CommandTask task) {
        long currentLogId = logHandler.getLogId();
        ArrayList<RaftLogHandler.Log> entries = new ArrayList<>();
        RaftLogHandler.Log entry = new RaftLogHandler.Log(
                currentLogId + 1,
                logHandler.getTerm(),
                task.command
        );
        entries.add(entry);
        byte[] requestBytes = new AppendEntry(entry.id(), entry.term(), entries).serialize();

        int count = 0;
        for (SelectionKey key : selector.keys()) {
            RaftClientSession session = (RaftClientSession) key.attachment();
            if (session != null && session.getLogId() == currentLogId) {
                count++;
                SocketChannel channel = session.getChannel();
                queueWrite(channel, ByteBuffer.wrap(requestBytes));
            }
        }
        task.logId = entry.id();
        task.majority = count / 2 + 1;
    }

    private ByteBuffer handleEntryRequest(RaftClientSession session, RequestEntry request) throws IOException {
        long currentLogId = logHandler.getLogId();
        long currentTerm = logHandler.getTerm();

        if (request.term() > currentTerm || (request.term() == currentTerm && request.id() > currentLogId)) {
            // Switching from controller to broker;
            manager.setRole(NodeRole.BROKER);
            return null;
        }

        // Handling a fresh follower
        if (request.id() == 0) {

            // Check if we have a snapshot
            Map.Entry<byte[], String> loadSnapshotBytesResult = loadSnapshotBytes();
            byte[] snapshotBytes = loadSnapshotBytesResult.getKey();

            if (snapshotBytes != null) {
                String fname = loadSnapshotBytesResult.getValue();

                long[] details = extractLogIdAndTerm(fname);
                return ByteBuffer.wrap(new InstallSnapshot(
                        snapshotBytes, details[0], details[1]
                ).serialize());
            }

            // Return all logs if available
            if (!logs.isEmpty()) {
                List<RaftLogHandler.Log> commands = new ArrayList<>();
                long firstLogId = logs.getFirst().id();
                long logTerm = logs.getFirst().term();

                for (RaftLogHandler.Log log : logs) {
                    commands.add(log);
                }

                return ByteBuffer.wrap(new AppendEntry(
                        firstLogId,
                        logTerm,
                        commands
                ).serialize());
            }

            // Follower is fresh
            session.setLogId(currentLogId);
            session.setTerm(currentTerm);

            // Leader hasn't processed a command yet.
            return ByteBuffer.wrap(new AppendEntry(
                    currentLogId,
                    currentTerm,
                    new ArrayList<>()
            ).serialize());
        }

        // Finding next log
        RaftLogHandler.Log firstLog = logs.get(0);

        if (request.id() < firstLog.id()) {
            Map.Entry<byte[], String> loadSnapshotBytesResult = loadSnapshotBytes();
            byte[] snapshotBytes = loadSnapshotBytesResult.getKey();
            String fname = loadSnapshotBytesResult.getValue();

            long[] details = extractLogIdAndTerm(fname);

            return ByteBuffer.wrap(new InstallSnapshot(
                    snapshotBytes, details[0], details[1]
            ).serialize());
        }

        if (request.id() == currentLogId) {
            RaftLogHandler.Log lastLog = logs.getLast();

            if (request.term() != lastLog.term()) {
                return ByteBuffer.wrap(new ErrorMessage(
                        RaftErrorType.INVALID_TERM, null
                ).serialize());
            }

            // Follower is up to date.
            session.setLogId(lastLog.id());
            session.setTerm(lastLog.term());
            // Return empty list since follower is up to date
            return ByteBuffer.wrap(new AppendEntry(
                    lastLog.id(),
                    lastLog.term(),
                    new ArrayList<>()
            ).serialize());
        }

        // Find all succeeding logs starting from request.id() + 1

        List<RaftLogHandler.Log> succeedingCommands = new ArrayList<>();
        long firstSucceedingLogId = -1;
        long firstSucceedingLogTerm = -1;

        for (RaftLogHandler.Log entry : logs) {
            if (entry.id() > request.id()) {
                if (succeedingCommands.isEmpty()) {
                    firstSucceedingLogId = entry.id();
                    firstSucceedingLogTerm = entry.term();
                }
                succeedingCommands.add(entry);
            }
        }

        if (succeedingCommands.isEmpty()) {
            return ByteBuffer.wrap(new ErrorMessage(
                    RaftErrorType.LOG_NOT_FOUND, null
            ).serialize());
        }

        return ByteBuffer.wrap(new AppendEntry(
                firstSucceedingLogId,
                firstSucceedingLogTerm,
                succeedingCommands).serialize()
        );
    }

    private ByteBuffer handleAppendEntryResponse(RaftClientSession session, AppendEntryResponse response) {
        final String debugPrefix = this.DEBUG_PREFIX + "[handleAppendEntryResponse] ";
        session.setLogId(response.id());
        session.setTerm(response.term());
        if (curCommandTask != null && response.id() == curCommandTask.logId) {
            // If true, we're currently looking to replicate a command across
            // the majority of the cluster.
            curCommandTask.count++;
            if (curCommandTask.count >= curCommandTask.majority) {
                curCommandTask.fut.complete(true);
                curCommandTask = null;
            }
        } else if (!logs.isEmpty()) {
            List<RaftLogHandler.Log> logs = new ArrayList<>();
            long firstLogId = -1;
            long firstLogTerm = -1;

            for (RaftLogHandler.Log log : this.logs) {
                if (log.id() > response.id()) {
                    if (firstLogId == -1) {
                        firstLogId = log.id();
                        firstLogTerm = log.term();
                    }
                    logs.add(log);
                }
            }

            queueWrite(session.getChannel(), ByteBuffer.wrap(new AppendEntry(
                    firstLogId,
                    firstLogTerm,
                    logs
            ).serialize()));
        }

        return null;
    }

    private AbstractMap.SimpleEntry<byte[], String> loadSnapshotBytes() throws IOException {
        Path snapshotDir = snapshotter.getDir();
        File[] snapshotFiles = snapshotDir.toFile().listFiles();
        Path snapshotPath = null;
        if (snapshotFiles != null && snapshotFiles.length > 0) {
            snapshotPath = snapshotFiles[0].toPath();
        }

        byte[] snapshotBytes = (snapshotPath == null) ? null : Files.readAllBytes(snapshotPath);
        String snapshotPathStr = (snapshotPath == null) ? null : snapshotPath.getFileName().toString();
        return new AbstractMap.SimpleEntry<>(snapshotBytes, snapshotPathStr);
    }

    private long[] extractLogIdAndTerm(String fname) {
        String[] parts = fname.replace(".snapshot", "").split("_", 2);
        long logId = Long.parseLong(parts[0]);
        long term = Long.parseLong(parts[1]);
        return new long[]{logId, term};
    }

    // === BROKER METHODS ===

    private ByteBuffer handleRequestVote(RequestVote requestVote) {
        final String debugPrefix = this.DEBUG_PREFIX + "[handleRequestVote] ";
        final long requestTerm = requestVote.term();
        if (requestTerm > manager.getCurrentTerm() && requestTerm > manager.getLastVotedTerm()) {
            manager.setCurrentTerm(requestTerm);

            if (requestVote.prevLogId() >= manager.getKVStore().getLogHandler().getLogId()) {
                manager.setLastVotedTerm(requestTerm);
                // Return the term that was granted (the updated current term)
                return ByteBuffer.wrap(new RequestVoteResponse(true, requestTerm).serialize());
            }

            manager.startElection();
        }

        return null;
    }

    // === SETTERS & GETTERS ===

    public Manager getManager() {
        return this.manager;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public boolean isRunning() {
        return this.isRunning;
    }

    private interface MessageHandler {
        ByteBuffer run() throws IOException;
    }

    private static class RaftClientSession extends ClientSession {
        private long logId = -1;
        private long term = -1;

        public RaftClientSession(SocketChannel channel) {
            super(channel);
        }

        public long getLogId() {
            return logId;
        }

        public void setLogId(long logId) {
            this.logId = logId;
        }

        public long getTerm() {
            return term;
        }

        public void setTerm(long term) {
            this.term = term;
        }
    }

    private static class CommandTask {
        public final Command command;
        public final CompletableFuture<Boolean> fut;
        public long logId = -1;
        public int majority;
        public int count;

        public CommandTask(Command command, CompletableFuture<Boolean> fut) {
            this.command = command;
            this.fut = fut;
        }

        @Override
        public String toString() {
            return "CommandTask{" +
                    "command=" + command +
                    ", fut=" + fut +
                    ", prevLogId=" + logId +
                    ", majority=" + majority +
                    ", count=" + count +
                    '}';
        }
    }
}
