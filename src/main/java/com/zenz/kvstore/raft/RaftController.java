package com.zenz.kvstore.raft;

import com.zenz.kvstore.ClientSession;
import com.zenz.kvstore.KVMapSnapshotter;
import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.messages.LogRequest;
import com.zenz.kvstore.messages.LogResponse;
import com.zenz.kvstore.messages.Message;

import java.io.File;
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

public class RaftController {
    private final String host;
    private final int port;
    private boolean running = true;

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

    public void stop() {
        if (!running) return;
        running = false;
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
        Message message = Message.deserialize(buffer.array());
        MessageType messageType = message.type();

        if (messageType.equals(MessageType.LOG_REQUEST)) {
            handleLogRequest(session, buffer, (LogRequest) message);
        } else if (messageType.equals(MessageType.HEARTBEAT_REQUEST)) {

        }
    }

    private ByteBuffer handleLogRequest(ClientSession session, ByteBuffer buffer, LogRequest request) throws IOException {
        // 1. Check if logId and term match current log id and term. If it does
        //    then send NULL
        // 2. Check if log id is in log array list. If it is, send the next log
        // 3. If log not in array list then send snapshot.

        long currentLogId = logHandler.getLogId();
        long currentTerm = logHandler.getTerm();

        // Handling a fresh follower
        if (request.logId() == 0 && request.term() == 0) {
            // Both nodes haven't processed any commands
            if (currentLogId == 0 && currentTerm == 1) {
                return ByteBuffer.wrap(
                        new LogResponse(
                                currentLogId,
                                currentTerm,
                                LogResponse.DataType.COMMAND,
                                new PutCommand("", new byte[0]),
                                null
                        ).serialize()
                );
            }

            // Check if we have a snapshot
            Path snapshotDir = snapshotter.getDir();
            File[] snapshotFiles = snapshotDir.toFile().listFiles();
            Path snapshotPath = null;
            if (snapshotFiles != null && snapshotFiles.length > 0) {
                snapshotPath = snapshotFiles[0].toPath();
            }

            if (snapshotPath != null) {
                byte[] snapshotBytes = Files.readAllBytes(snapshotPath);
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

            // If no snapshot, send first log
            RaftLogHandler.Log log = logs.get(0);
            return ByteBuffer.wrap(
                    new LogResponse(
                            log.id(),
                            log.term(),
                            LogResponse.DataType.COMMAND,
                            log.command(),
                            null
                    ).serialize()
            );
        }

        if (request.logId() == currentLogId && request.term() == currentTerm) {
            return ByteBuffer.wrap(new byte[0]);
        }

        if (request.logId() == currentLogId && request.term() != currentTerm) {
            return ByteBuffer.wrap("ERROR Invalid term".getBytes(StandardCharsets.UTF_8));
        }

        // Searching for next log
        Path fpath = logHandler.getLogger().getPath();
        ArrayList<RaftLogHandler.Log> logs = logHandler.deserialize(fpath);

        if (logs.isEmpty()) {
        }

        return ByteBuffer.wrap(new byte[0]);
    }

    public void handleWrite(SelectionKey key) throws IOException {
    }

    private void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;
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
