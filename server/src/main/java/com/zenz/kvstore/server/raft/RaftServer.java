package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.server.ClientSession;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.logging.handlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.messages.*;

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

public class RaftServer {
    // Server
    private final RaftManager raftManager;
    private final InetSocketAddress address;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
    private boolean isRunning = false;

    // Controller
    private final RaftLogHandler logHandler;
    private final KVMapSnapshotter snapshotter;
    private final ArrayList<RaftLogHandler.Log> logs = new ArrayList<>();
    private final ConcurrentLinkedQueue<CommandTask> commandTasks = new ConcurrentLinkedQueue<>();
    private CommandTask curCommandTask;

    public RaftServer(InetSocketAddress address, RaftManager raftManager) {
        this.address = address;
        this.raftManager = raftManager;

        logHandler = (RaftLogHandler) raftManager.getKVStore().getLogHandler();
        snapshotter = raftManager.getKVStore().getSnapshotter();
    }

    public void start() throws Exception {
        if (isRunning) return;

        isRunning = true;

        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(address);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (isRunning) {
            int readyCount = selector.select();

            if (raftManager.getRole().equals(NodeRole.CONTROLLER)) {
                checkPendingTask();
            }

            if (readyCount == 0) {
                continue;
            }

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

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
        if (!isRunning) {
            return;
        }

        isRunning = false;

        if (selector != null) {
            selector.wakeup();
        }

        if (selector != null) {
            for (SelectionKey key : selector.keys()) {
                cleanup(key);
            }
            selector.close();
        }

        if (serverChannel != null) {
            serverChannel.close();
        }

        pendingWrites.clear();
    }

    public void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;

        SelectableChannel channel = key.channel();
        channel.close();
        key.cancel();
        pendingWrites.remove(channel);
    }

    private void convertToController() {}

    private void convertToBroker() {}

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
        RaftClientSession session = (RaftClientSession) key.attachment();
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
                cleanup(key);
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
        boolean processed = processData(session, readBuffer);
        readBuffer.flip();

        if (processed) {
            readBuffer.clear();
            readBuffer.rewind();
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
            int bytesWritten = client.write(buffer);

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
        BaseMessage message;
        try {
            message = BaseMessage.deserialize(buffer);
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
        ByteBuffer responseBuffer = null;

        // Controller
        if (messageType.equals(MessageType.REQUEST_ENTRY)) {
            responseBuffer = handleMessage(
                    () -> handleEntryRequest(session, (RequestEntry) message),
                    NodeRole.CONTROLLER
            );
        } else if (messageType.equals(MessageType.APPEND_ENTRY_RESPONSE)) {
            responseBuffer = handleMessage(
                    () -> handleAppendEntryResponse(session, (AppendEntryResponse) message),
                    NodeRole.CONTROLLER
            );
        } else if (messageType.equals(MessageType.HEARTBEAT_REQUEST)) {
            responseBuffer = handleMessage(() -> ByteBuffer.wrap(new HeartbeatResponse().serialize()), NodeRole.CONTROLLER);
        }
        // Broker
        else if (messageType.equals(MessageType.REQUEST_VOTE)) {
            responseBuffer = handleMessage(() -> handleRequestVote((RequestVote) message), NodeRole.BROKER);
        } else if (messageType.equals(MessageType.LEADER_ELECTED)) {
            LeaderElected msg = (LeaderElected) message;
            raftManager.setLastSeenTerm(msg.term());
            raftManager.handleLeaderElected((LeaderElected) message);
            cleanup(session.getChannel().keyFor(selector));
        }
        // Both
        else if (messageType.equals(MessageType.REGISTER)) {
            raftManager.handleRegisterMessage((RegisterMessage) message);
        }

        if (responseBuffer != null) {
            queueWrite(session.getChannel(), responseBuffer);
        }

        return true;
    }

    /**
     * Ensures this current node's role is the required role to handle the message.
     * If it isn't then a redirect message is returned. If it is then the return buffer
     * of the passed handler is returned.
     * @param checkedRunnable
     * @param role
     * @return
     */
    private ByteBuffer handleMessage(MessageHandler checkedRunnable, NodeRole role) {
        if (!raftManager.getRole().equals(role)) {
            if (role.equals(NodeRole.CONTROLLER)) {
                return ByteBuffer.wrap(
                        new RedirectMessage(
                                raftManager.getControllerConfig(),
                                NodeRole.CONTROLLER
                        ).serialize()
                );
            }
        }

        try {
            return checkedRunnable.run();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
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
        for (RaftLogHandler.Log logEntry : RaftLogHandler.deserialize(path)) {
            logs.add(logEntry);
        }
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

        if (request.term() > currentTerm) {
            // Switching from controller to broker;
            raftManager.setRole(NodeRole.BROKER);
            raftManager.startBrokerClients();
            return ByteBuffer.wrap(new ErrorMessage(
                    RaftErrorType.GREATER_TERM, null
            ).serialize());
        }

        if (request.term() == currentTerm && request.id() > currentLogId) {
            // Switching from controller to broker;
            raftManager.setRole(NodeRole.BROKER);
            raftManager.startBrokerClients();
            return ByteBuffer.wrap(new ErrorMessage(
                    RaftErrorType.GREATER_LOG_ID, null
            ).serialize());
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

    private ByteBuffer handleRequestVote(RequestVote msg) throws IOException {
        if (msg.term() > raftManager.getLastSeenTerm() && msg.term() > raftManager.getVotedTerm()) {
            raftManager.setLastSeenTerm(msg.term());

            if (msg.prevLogId() >= raftManager.getKVStore().getLogHandler().getLogId()) {
                raftManager.setVotedTerm(msg.term());
                RequestVoteResponse response = new RequestVoteResponse(true, msg.term());
                return ByteBuffer.wrap(response.serialize());
            }

            raftManager.initiateElection();
        }

        return null;
    }

    // === SETTERS & GETTERS ===

    public RaftManager getRaftManager() {
        return raftManager;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public boolean isRunning() {
        return isRunning;
    }

    private class RaftClientSession extends ClientSession {
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

    private interface MessageHandler {
        ByteBuffer run() throws IOException;
    }

    private class CommandTask {
        public final Command command;
        public final CompletableFuture<Boolean> fut;
        public long logId = -1;
        public int majority;
        public int count;

        public CommandTask(
                Command command,
                CompletableFuture<Boolean> fut
        ) {
            this.command = command;
            this.fut = fut;
        }

        public CommandTask(
                Command command,
                CompletableFuture<Boolean> fut,
                long logId,
                int majority,
                int count
        ) {
            this.command = command;
            this.fut = fut;
            this.logId = logId;
            this.majority = majority;
            this.count = count;
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
