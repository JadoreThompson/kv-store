package com.zenz.kvstore.raft;

import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.RaftErrorType;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.raft.messages.*;
import com.zenz.kvstore.requests.LogBroadcastRequest;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RaftController {
    private final String host;
    private final int port;
    private boolean running = false;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();

    private RaftLogHandler logHandler;
    private KVMapSnapshotter snapshotter;
    private ArrayList<RaftLogHandler.Log> logs;

    private long logId;
    private int majority;
    private int count;
    private final ArrayList<SocketChannel> followers = new ArrayList<>();
    private CompletableFuture<Boolean> fut;

    public RaftController(String host, int port, RaftLogHandler logHandler, KVMapSnapshotter snapshotter) {
        this.host = host;
        this.port = port;
        this.logHandler = logHandler;
        this.snapshotter = snapshotter;
    }

    public void start() throws IOException {
        if (running) return;

        loadLogs();

        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(host, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        running = true;

        while (running) {
            int readyCount = selector.select();
            if (readyCount == 0) continue;

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
                } catch (IOException e) {
                    System.err.println("Connection error: " + e.getMessage());
                    cleanup(key);
                }
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

        Path path = logHandler.getLogger().getPath();
        for (RaftLogHandler.Log logEntry : RaftLogHandler.deserialize(path)) {
            logs.add(logEntry);
        }
    }

    public void stop() throws IOException {
        if (!running) return;

        running = false;

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

        logs = null;
        pendingWrites.clear();
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();

        if (channel == null) return;

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setKeepAlive(true);

        SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_READ);
        selectionKey.attach(new ClientSession(channel));
    }

    private void handleRead(SelectionKey key) throws IOException {
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
        buffer.flip();
        int position = buffer.position();
        boolean consumed = processData(session, buffer);
        if (consumed) {
            buffer.compact();
        } else {
            buffer.position(position);
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
        long currentLogId = logHandler.getLogId();
        ArrayList<Command> commands = new ArrayList<>();
        commands.add(command);
        byte[] requestBytes = new AppendEntry(
                currentLogId + 1,
                logHandler.getTerm(),
                commands
        ).serialize();
        int count = 0;

        for (SelectionKey key : selector.keys()) {
            ClientSession session = (ClientSession) key.attachment();
            if (session.logId == currentLogId) {
                count++;
                SocketChannel channel = session.getChannel();
                followers.add(channel);
                queueWrite(channel, ByteBuffer.wrap(requestBytes));
            }
        }

        logId = currentLogId;
        majority = count / 2 + 1;
        this.fut = fut;
    }

    private ByteBuffer handleEntryRequest(ClientSession session, RequestEntry request) throws IOException {
        long currentLogId = logHandler.getLogId();
        long currentTerm = logHandler.getTerm();


        // Leader needs to be dethroned and converted to a follower.
        if (request.term() > currentTerm) {
            stop();
            return null;
        }

        // Leader needs to be dethroned and converted to a follower.
        if (request.term() == currentTerm && request.id() > currentLogId) {
            stop();
            return null;
        }

        // Handling a fresh follower
        if (request.id() == 0) {
            // Check if we have a snapshot
//            byte[] snapshotBytes = loadSnapshotBytes();
            Map.Entry<byte[], String> loadSnapshotBytesResult = loadSnapshotBytes();
            byte[] snapshotBytes = loadSnapshotBytesResult.getKey();

            if (snapshotBytes != null) {
//                return ByteBuffer.wrap(new AppendSnapshot(
//                        snapshotBytes
//                ).serialize());
                String fname = loadSnapshotBytesResult.getValue();
                long[] details = extractLogIdAndTerm(fname);

//                return ByteBuffer.wrap(new AppendSnapshot(
//                        snapshotBytes
//                ).serialize());
                return ByteBuffer.wrap(new AppendSnapshotV2(
                        snapshotBytes, details[0], details[1]
                ).serialize());
            }

            // Return all logs if available
            if (!logs.isEmpty()) {
//                List<Command> commands = new ArrayList<>();
//                long firstLogId = logs.getFirst().id();
//                long logTerm = logs.getFirst().term();
//
//                for (RaftLogHandler.Log log : logs) {
//                    commands.add(log.command());
//                }
//
//                return ByteBuffer.wrap(new AppendEntry(
//                        firstLogId,
//                        logTerm,
//                        commands
//                ).serialize());
                List<RaftLogHandler.Log> commands = new ArrayList<>();
                long firstLogId = logs.getFirst().id();
                long logTerm = logs.getFirst().term();

                for (RaftLogHandler.Log log : logs) {
                    commands.add(log);
                }

                return ByteBuffer.wrap(new AppendEntryV2(
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
//            byte[] snapshotBytes = loadSnapshotBytes();
            Map.Entry<byte[], String> loadSnapshotBytesResult = loadSnapshotBytes();
            byte[] snapshotBytes = loadSnapshotBytesResult.getKey();
            String fname = loadSnapshotBytesResult.getValue();
            long[] details = extractLogIdAndTerm(fname);

//            return ByteBuffer.wrap(new AppendSnapshot(
//                    snapshotBytes
//            ).serialize());
            return ByteBuffer.wrap(new AppendSnapshotV2(
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
//        List<Command> succeedingCommands = new ArrayList<>();
//        long firstSucceedingLogId = -1;
//        long firstSucceedingLogTerm = -1;
//
//        for (RaftLogHandler.Log entry : logs) {
//            if (entry.id() > request.id()) {
//                if (succeedingCommands.isEmpty()) {
//                    firstSucceedingLogId = entry.id();
//                    firstSucceedingLogTerm = entry.term();
//                }
//                succeedingCommands.add(entry.command());
//            }
//        }
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
            // No succeeding logs found - shouldn't happen due to prev guarding checks
            return ByteBuffer.wrap(new ErrorMessage(
                    RaftErrorType.LOG_NOT_FOUND, null
            ).serialize());
        }

//        return ByteBuffer.wrap(new AppendEntry(
//                firstSucceedingLogId,
//                firstSucceedingLogTerm,
//                succeedingCommands).serialize()
//        );
        return ByteBuffer.wrap(new AppendEntryV2(
                firstSucceedingLogId,
                firstSucceedingLogTerm,
                succeedingCommands).serialize()
        );
    }

    private ByteBuffer handleAppendEntryResponse(ClientSession session, AppendEntryResponse response) {
        session.setLogId(response.id());
        session.setTerm(response.term());

        if (logId == 0 || response.id() != logId) {
            if (!logs.isEmpty()) {
//                List<Command> commands = new ArrayList<>();
//                long firstLogId = -1;
//                long logTerm = -1;
//
//                for (RaftLogHandler.Log log : logs) {
//                    if (log.id() > response.id()) {
//                        if (firstLogId == -1) {
//                            firstLogId = log.id();
//                            logTerm = log.term();
//                        }
//                        commands.add(log.command());
//                    }
//                }
//
//                queueWrite(session.getChannel(), ByteBuffer.wrap(new AppendEntry(
//                        firstLogId,
//                        logTerm,
//                        commands
//                ).serialize()));
                List<RaftLogHandler.Log> commands = new ArrayList<>();
                long firstLogId = -1;
                long logTerm = -1;

                for (RaftLogHandler.Log log : logs) {
                    if (log.id() > response.id()) {
                        if (firstLogId == -1) {
                            firstLogId = log.id();
                            logTerm = log.term();
                        }
                        commands.add(log);
                    }
                }

                queueWrite(session.getChannel(), ByteBuffer.wrap(new AppendEntryV2(
                        firstLogId,
                        logTerm,
                        commands
                ).serialize()));
            }
        } else if (response.id() == logId && majority > 0) {
            // We're currently looking to confirm that the majority
            // of the followers have committed a command.
            count++;
            if (count >= majority) {
                fut.complete(true);
                fut = null;
                majority = 0;
                count = 0;
                followers.clear();
            }
        }


        return null;
    }

//    private byte[] loadSnapshotBytes() throws IOException {
//        Path snapshotDir = snapshotter.getDir();
//        File[] snapshotFiles = snapshotDir.toFile().listFiles();
//        Path snapshotPath = null;
//        if (snapshotFiles != null && snapshotFiles.length > 0) {
//            snapshotPath = snapshotFiles[0].toPath();
//        }
//
//        return (snapshotPath == null) ? null : Files.readAllBytes(snapshotPath)
//    }

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
        String[] parts = fname.split("_", 2);
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

    private void handleWrite(SelectionKey key) throws IOException {
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
        pendingWrites.remove(channel);
        followers.remove(channel);
        if (majority > 0) {
            majority--;
        }
        channel.close();
        key.cancel();
    }

    public boolean isRunning() {
        return running;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public RaftLogHandler getLogHandler() {
        return logHandler;
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
}


