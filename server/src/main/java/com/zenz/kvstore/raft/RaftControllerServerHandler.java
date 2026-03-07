package com.zenz.kvstore.raft;

import com.zenz.kvstore.*;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.raft.messages.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RaftControllerServerHandler implements SocketHandler {
    private RaftLogHandler logHandler;
    private KVMapSnapshotter snapshotter;
    private final ArrayList<RaftLogHandler.Log> logs = new ArrayList<>();
    private ConcurrentLinkedQueue<CommandTask> commandTasks = new ConcurrentLinkedQueue<>();
    private CommandTask curCommandTask;
    private final ArrayList<SocketChannel> followers = new ArrayList<>();
    private RaftManager manager;
    private SocketServer server;

    private Selector selector;
    private Map<SocketChannel, Queue<ByteBuffer>> pendingWrites;
    private final String DEBUG_PREFIX;

    public RaftControllerServerHandler(
            SocketServer server,
            RaftLogHandler logHandler,
            KVMapSnapshotter snapshotter,
            RaftManager manager
    ) {
        this.server = server;
        this.logHandler = logHandler;
        this.snapshotter = snapshotter;
        this.manager = manager;
        DEBUG_PREFIX = "[RaftControllerServerHandler]";
    }

    public void init() throws IOException {
        pendingWrites = server.getPendingWrites();
        selector = server.getSelector();
        loadLogs();
    }

    @Override
    public void handleWakeUp() {
        final String debugPrefix = DEBUG_PREFIX + "[handleWakeUp] ";
        // Process pending command tasks from the queue
        if (curCommandTask == null && !commandTasks.isEmpty()) {
            CommandTask task = commandTasks.poll();
            if (task != null) {
                curCommandTask = task;
                // Trigger the private handleCommand to broadcast to followers
                handleCommand(curCommandTask);
            }
        }
    }

    /**
     * Loads all logs from the current log file into memory
     *
     * @throws IOException
     */
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

    public void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();

        if (channel == null) return;

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setKeepAlive(true);

        SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_READ);
        selectionKey.attach(new ClientSession(channel));
    }

    public void handleRead(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();

        ByteBuffer buffer = session.getReadBuffer();
        int bytesRead = client.read(buffer);

        if (bytesRead == -1) {
            cleanup(key);
            return;
        }

        if (bytesRead == 0) {
            return;
        }

        // Process the data
        int prevPosition = buffer.position();
        buffer.flip();
        boolean consumed = processData(session, buffer);
        buffer.flip();
        if (consumed) {
            buffer.clear();
            buffer.rewind();
        } else {
            buffer.position(prevPosition);
        }
    }

    private boolean processData(ClientSession session, ByteBuffer buffer) throws IOException {
        BaseMessage message;
        try {
            message = BaseMessage.deserialize(buffer);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            queueWrite(
                    session.getChannel(),
                    ByteBuffer.wrap(new ErrorMessage(
                            RaftErrorType.INVALID_MESSAGE_TYPE, e.getMessage()
                    ).serialize())
            );
            return true;
        }

        if (message == null) {
            return false;
        }

        MessageType messageType = message.type();
        ByteBuffer responseBuffer = null;

        if (messageType.equals(MessageType.REQUEST_ENTRY)) {
            responseBuffer = handleEntryRequest(session, (RequestEntry) message);
        } else if (messageType.equals(MessageType.APPEND_ENTRY_RESPONSE)) {
            responseBuffer = handleAppendEntryResponse(session, (AppendEntryResponse) message);
        } else if (messageType.equals(MessageType.HEARTBEAT_REQUEST)) {
            responseBuffer = ByteBuffer.wrap(new HeartbeatResponse().serialize());
        }

        if (responseBuffer != null) {
            queueWrite(session.getChannel(), responseBuffer);
        }

        return true;
    }

    /**
     * Broadcast the command to all followers. Awaits for majority to confirm
     * that the command has been commited.
     *
     * @param command
     */
    public void handleCommand(Command command, CompletableFuture<Boolean> fut) {
        final String debugPrefix = DEBUG_PREFIX + "[handleCommand][public] ";

        CommandTask task = new CommandTask(command, fut);
        commandTasks.add(task);
        selector.wakeup();
    }

    private void handleCommand(CommandTask task) {
        final String debugPrefix = DEBUG_PREFIX + "[handleCommand][private]";

        // Forming the request
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
            ClientSession session = (ClientSession) key.attachment();
            if (session != null && session.logId == currentLogId) {
                count++;
                SocketChannel channel = session.getChannel();
                followers.add(channel);
                queueWrite(channel, ByteBuffer.wrap(requestBytes));
            }
        }

        task.logId = entry.id();
        task.majority = count / 2 + 1;
    }


    private ByteBuffer handleEntryRequest(ClientSession session, RequestEntry request) throws IOException {
        long currentLogId = logHandler.getLogId();
        long currentTerm = logHandler.getTerm();

        if (request.term() > currentTerm) {
            return ByteBuffer.wrap(new ErrorMessage(
                    RaftErrorType.GREATER_TERM, null
            ).serialize());
        }

        if (request.term() == currentTerm && request.id() > currentLogId) {
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

    private ByteBuffer handleAppendEntryResponse(ClientSession session, AppendEntryResponse response) {
        final String debugPrefix = DEBUG_PREFIX + "[handleAppendEntryResponse] ";
        session.setLogId(response.id());
        session.setTerm(response.term());

        if (curCommandTask != null && response.id() == curCommandTask.logId) {
            // If true, we're currently looking to replicate a command across
            // the majority of the cluster.
            curCommandTask.count++;
            if (curCommandTask.count >= curCommandTask.majority) {
                followers.clear();
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

    private void queueWrite(SocketChannel channel, ByteBuffer buffer) {
        Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(buffer.duplicate());

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
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

    private void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;

        SelectableChannel channel = key.channel();
        server.cleanup(key);

        if (followers.remove(channel) && curCommandTask != null) {
            curCommandTask.majority--;
            if (curCommandTask.count >= curCommandTask.majority) {
                followers.clear();
                curCommandTask.fut.complete(true);
                curCommandTask = null;
            }
        }
    }

    public RaftLogHandler getLogHandler() {
        return logHandler;
    }

    public RaftManager getManager() {
        return manager;
    }

    private class ClientSession extends com.zenz.kvstore.ClientSession {
        private long logId = -1;
        private long term = -1;

        public ClientSession(SocketChannel channel) {
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
    }
}


