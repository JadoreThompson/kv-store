package com.zenz.kvstore.raft;

import com.zenz.kvstore.ClientSession;
import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.RequestType;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.requests.BaseRequest;
import com.zenz.kvstore.requests.LogRequest;
import com.zenz.kvstore.responses.ErrorResponse;
import com.zenz.kvstore.responses.HeartbeatResponse;
import com.zenz.kvstore.responses.LogResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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

    public RaftController(String host, int port, RaftLogHandler logHandler, KVMapSnapshotter snapshotter) {
        this.host = host;
        this.port = port;
        this.logHandler = logHandler;
        this.snapshotter = snapshotter;
    }

    public void start() throws IOException {
        if (running) return;

        loadLogs();

        // Open selector
        selector = Selector.open();

        // Create and configure server channel
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(host, port));

        // Register for ACCEPT events
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        running = true;

        // Main event loop
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
        Path path = logHandler.getLogger().getPath();
        logs = RaftLogHandler.deserialize(path);
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
        buffer.flip();
        processData(session, buffer);
    }

    public void processData(ClientSession session, ByteBuffer buffer) throws IOException {
        BaseRequest request = BaseRequest.deserialize(buffer.array());
        RequestType requestType = request.type();

        ByteBuffer responseBuffer = null;
        if (requestType.equals(RequestType.LOG)) {
            responseBuffer = handleLogRequest(session, buffer, (LogRequest) request);
        } else if (requestType.equals(RequestType.HEARTBEAT)) {
            responseBuffer = ByteBuffer.wrap(new HeartbeatResponse().serialize());
        }

        if (responseBuffer != null) {
            queueWrite(session.getChannel(), responseBuffer);
        }
    }

    private ByteBuffer handleLogRequest(ClientSession session, ByteBuffer buffer, LogRequest request) throws IOException {
        long currentLogId = logHandler.getLogId();
        long currentTerm = logHandler.getTerm();

        if (request.term() > currentTerm) {
            return ByteBuffer.wrap(new ErrorResponse(
                    "Term is greater than current term " + currentTerm
            ).serialize());
        }

        if (request.logId() > currentLogId) {
            return ByteBuffer.wrap(new ErrorResponse(
                    "Log id is greater than current log id " + currentLogId
            ).serialize());

        }

        // Handling a fresh follower
        if (request.logId() == 0) {
            // Check if we have a snapshot
            byte[] snapshotBytes = loadSnapshotBytes();

            if (snapshotBytes != null) {
                return ByteBuffer.wrap(
                        new LogResponse(
                                currentLogId,
                                currentTerm,
                                LogResponse.DataType.SNAPSHOT,
                                null,
                                snapshotBytes
                        ).serialize()
                );
            }

            RaftLogHandler.Log log = !logs.isEmpty() ? logs.getFirst() : null;
            if (log != null) {
                return ByteBuffer.wrap(new LogResponse(
                        log.id(),
                        log.term(),
                        LogResponse.DataType.COMMAND,
                        log.command(),
                        null
                ).serialize());
            }

            return ByteBuffer.wrap(new LogResponse(
                    currentLogId,
                    currentTerm,
                    LogResponse.DataType.COMMAND,
                    null,
                    null
            ).serialize());
        }

        // Finding next log
        RaftLogHandler.Log log = logs.get(0);

        if (request.logId() < log.id()) {
            byte[] snapshotBytes = loadSnapshotBytes();

            return ByteBuffer.wrap(
                    new LogResponse(
                            currentLogId,
                            currentTerm,
                            LogResponse.DataType.SNAPSHOT,
                            null,
                            snapshotBytes
                    ).serialize()
            );
        }

        if (request.logId() == currentLogId) {
            RaftLogHandler.Log lastLog = logs.getLast();

            if (request.term() != lastLog.term()) {
                return ByteBuffer.wrap(new ErrorResponse(
                        "Invalid term for log id=" + request.logId()
                ).serialize());
            }

            return ByteBuffer.wrap(new LogResponse(
                    lastLog.id(),
                    lastLog.term(),
                    LogResponse.DataType.COMMAND,
                    lastLog.command(),
                    null
            ).serialize());
        }

        long nextLogId = request.logId() + 1;

        for (RaftLogHandler.Log logEntry : logs) {
            if (logEntry.id() == nextLogId) {
                return ByteBuffer.wrap(
                        new LogResponse(
                                logEntry.id(),
                                logEntry.term(),
                                LogResponse.DataType.COMMAND,
                                logEntry.command(),
                                null
                        ).serialize()
                );
            }
        }

        // Log id is too large
        return ByteBuffer.wrap(new ErrorResponse("Failed to find next log").serialize());
    }

    private byte[] loadSnapshotBytes() throws IOException {
        Path snapshotDir = snapshotter.getDir();
        File[] snapshotFiles = snapshotDir.toFile().listFiles();
        Path snapshotPath = null;
        if (snapshotFiles != null && snapshotFiles.length > 0) {
            snapshotPath = snapshotFiles[0].toPath();
        }

        return (snapshotPath == null) ? null : Files.readAllBytes(snapshotPath);
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
        pendingWrites.remove(channel);
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
}
