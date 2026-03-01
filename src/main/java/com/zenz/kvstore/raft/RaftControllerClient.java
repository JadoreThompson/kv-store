package com.zenz.kvstore.raft;

import com.zenz.kvstore.*;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.RaftLogHandler;
import com.zenz.kvstore.raft.messages.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RaftControllerClient {
    private final InetSocketAddress controllerAddress;
    private Selector selector;
    private SocketChannel socketChannel;
    private Queue<ByteBuffer> pendingWrites = new LinkedList<>();
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private boolean isRunning;

    private KVStore store;
    private RaftLogHandler logHandler;
    private ArrayList<RaftLogHandler.Log> logs = new ArrayList<>();
    private CompletableFuture<Boolean> onDisconnect;

    public RaftControllerClient(String host, int port, KVStore store, RaftLogHandler logHandler) {
        controllerAddress = new InetSocketAddress(host, port);
        this.store = store;
        this.logHandler = logHandler;
    }

    public void start() throws IOException {
        if (isRunning) return;

        selector = Selector.open();
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(controllerAddress);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);

        isRunning = true;

        while (isRunning) {
            int readyCount = selector.select();
            if (readyCount == 0) continue;

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) continue;

                try {
                    if (key.isConnectable()) {
                        if (((SocketChannel) key.channel()).finishConnect()) {
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
    }

    public void stop() throws IOException {
        if (!isRunning) return;

        isRunning = false;
        socketChannel.close();
        selector.close();
        pendingWrites.clear();
        readBuffer.clear();
        logs.clear();
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

//    private void handleRead(SelectionKey key) throws IOException {
//        final String M = "[handleRead] ";
//        System.out.println(M + "handling read");
//        SocketChannel channel = (SocketChannel) key.channel();
//
//        while (true) {
//            if (readBuffer.position() >= readBuffer.capacity() - 1) {
//                System.out.println(M + "resizing buffer");
//                ByteBuffer newReadBuffer = ByteBuffer.allocate((int) (readBuffer.capacity() * 1.6));
//
//                newReadBuffer.put(readBuffer);
//                readBuffer = newReadBuffer;
//            }
//
//            int bytesRead = channel.read(readBuffer);
//
//            if (bytesRead == -1) {
//                cleanup(key);
//                return;
//            }
//
//            if (readBuffer.position() < readBuffer.capacity() - 1) {
//                break;
//            }
//        }
//
//        readBuffer.flip();
//        int position = readBuffer.position();
//        boolean processed = processData(key);
//        if (processed) {
//            readBuffer.compact();
//        } else {
//            readBuffer.position(position);
//        }
//    }

    private void handleRead(SelectionKey key) throws IOException {
        final String M = "[handleRead] ";
        System.out.println(M + "entered");

        SocketChannel channel = (SocketChannel) key.channel();
        System.out.println(M + "channel=" + channel);

        while (true) {
            System.out.println(M + "loop start | position=" + readBuffer.position() +
                    " capacity=" + readBuffer.capacity());

            if (readBuffer.position() >= readBuffer.capacity() - 1) {
                System.out.println(M + "resizing buffer");

                int oldCap = readBuffer.capacity();
                int newCap = (int) (oldCap * 1.6);

                ByteBuffer newReadBuffer = ByteBuffer.allocate(newCap);

                readBuffer.flip();
                newReadBuffer.put(readBuffer);
                readBuffer = newReadBuffer;

                System.out.println(M + "buffer resized | oldCapacity=" + oldCap +
                        " newCapacity=" + newCap);
            }

            int bytesRead = channel.read(readBuffer);
            System.out.println(M + "bytesRead=" + bytesRead);

            if (bytesRead == -1) {
                System.out.println(M + "remote closed connection");
                cleanup(key);
                return;
            }

            if (bytesRead == 0) {
                System.out.println(M + "no more data available right now");
            }

            if (readBuffer.position() < readBuffer.capacity() - 1) {
                System.out.println(M + "buffer not full, breaking read loop");
                break;
            }
        }

        System.out.println(M + "flipping buffer for processing");
        readBuffer.flip();

        System.out.println(M + "buffer state after flip | position=" +
                readBuffer.position() + " limit=" + readBuffer.limit());

        int position = readBuffer.position();

        System.out.println(M + "invoking processData");
        boolean processed = processData(key);

        System.out.println(M + "processData returned=" + processed);

        if (processed) {
            System.out.println(M + "compacting buffer");
            readBuffer.compact();

            System.out.println(M + "buffer after compact | position=" +
                    readBuffer.position() + " limit=" + readBuffer.limit());
        } else {
            System.out.println(M + "message incomplete, restoring position=" + position);
            readBuffer.position(position);
        }

        System.out.println(M + "exit");
    }

    private boolean processData(SelectionKey key) throws IOException {
        final String M = "[processData] ";
        BaseMessage message;

        try {
            message = BaseMessage.deserialize(readBuffer);
        } catch (IllegalArgumentException e) {
            return true;
        }

        if (message == null) return false;

        ByteBuffer responseBuffer = null;
        if (message.type().equals(MessageType.APPEND_ENTRY)) {
//            responseBuffer = handleAppendEntry(key, (AppendEntry) message);
            responseBuffer = handleAppendEntry(key, (AppendEntryV2) message);
        } else if (message.type().equals(MessageType.APPEND_SNAPSHOT)) {
//            responseBuffer = handleAppendSnapshot(key, (AppendSnapshot) message);
            System.out.println(M + "calling append snapshot");
            responseBuffer = handleAppendSnapshot(key, (AppendSnapshotV2) message);
        }

        if (responseBuffer != null) {
            queueWrite((SocketChannel) key.channel(), responseBuffer);
        }

        return true;
    }

    //    private ByteBuffer handleAppendEntry(SelectionKey key, AppendEntry message) throws IOException {
//        long curLogId = logHandler.getLogId();
//        long curTerm = logHandler.getTerm();
//
//        // Nothing to process. We're up to date.
//        if (curLogId == message.id() && curTerm == message.term()) return null;
//
//        // Processing all commands.
//        List<Command> commands = message.commands();
//        for (Command command : commands) {
//            if (command.type().equals(CommandType.PUT)) {
//                PutCommand comm = (PutCommand) command;
//                store.put(comm.key(), comm.value());
//            }
//        }
//
//        return ByteBuffer.wrap(new AppendEntryResponse(
//                logHandler.getLogId(), logHandler.getTerm(), true
//        ).serialize());
//    }
    private ByteBuffer handleAppendEntry(SelectionKey key, AppendEntryV2 message) throws IOException {
        long curLogId = logHandler.getLogId();
        long curTerm = logHandler.getTerm();

        // Nothing to process. We're up to date.
        if (curLogId == message.id() && curTerm == message.term()) return null;

        // Processing all commands.
        List<RaftLogHandler.Log> commands = message.entries();
        for (RaftLogHandler.Log command : commands) {
            if (command.term() != logHandler.getTerm()) logHandler.setTerm(command.term());
            if (command.command().type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command.command();
                store.put(comm.key(), comm.value());
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
     * @param key
     * @param message
     * @return
     * @throws IOException
     */
//    private ByteBuffer handleAppendSnapshot(SelectionKey key, AppendSnapshotV2 message) throws IOException {
//        long lastLogId = message.logId();
//        long lastTerm = message.term();
//        byte[] snapshot = message.snapshot();
//
//        KVMapSnapshotter snapshotter = store.getSnapshotter();
//        Path snapshotDir = snapshotter.getDir();
//        Path snapshotPath = snapshotDir.resolve(lastLogId + "_" + lastTerm + ".snapshot");
//        Files.createFile(snapshotPath);
//        try (FileChannel fileChannel = FileChannel.open(snapshotPath, StandardOpenOption.WRITE)) {
//            fileChannel.write(ByteBuffer.wrap(snapshot));
//        }
//
//        File[] files = snapshotDir.toFile().listFiles();
//        for (File file : files) {
//            if (!file.getName().equals(snapshotPath.getFileName().toString())) file.delete();
//        }
//
//        Path logFpath = store.getLogHandler().getLogger().getPath();
//        Files.delete(logFpath);
//        Files.createFile(logFpath);
//        store.getLogHandler().setLogger(new WALogger(logFpath));
//
////        return ByteBuffer.wrap(new byte[0]);
//        return ByteBuffer.wrap(new AppendEntryResponse(lastLogId, lastTerm, true).serialize());
//    }
    private ByteBuffer handleAppendSnapshot(SelectionKey key, AppendSnapshotV2 message) throws IOException {
        final String M = "[handleAppendSnapshot] ";

        System.out.println(M + "invoked");

        long lastLogId = message.logId();
        long lastTerm = message.term();
        byte[] snapshot = message.snapshot();

        System.out.println(M + "Snapshot metadata:");
        System.out.println(M + "lastLogId=" + lastLogId);
        System.out.println(M + "lastTerm=" + lastTerm);
        System.out.println(M + "snapshotSize=" + (snapshot != null ? snapshot.length : -1));

        KVMapSnapshotter snapshotter = store.getSnapshotter();
        Path snapshotDir = snapshotter.getDir();

        System.out.println(M + "Snapshot directory: " + snapshotDir);

        Path snapshotPath = snapshotDir.resolve(lastLogId + "_" + lastTerm + ".snapshot");

        System.out.println(M + "Creating snapshot file: " + snapshotPath);
        Files.createFile(snapshotPath);

        try (FileChannel fileChannel = FileChannel.open(snapshotPath, StandardOpenOption.WRITE)) {
            System.out.println(M + "Writing snapshot bytes...");
            fileChannel.write(ByteBuffer.wrap(snapshot));
            System.out.println(M + "Snapshot write complete");
        }

        System.out.println(M + "Cleaning old snapshots...");
        File[] files = snapshotDir.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.getName().equals(snapshotPath.getFileName().toString())) {
                    System.out.println(M + "Deleting old snapshot: " + file.getAbsolutePath());
                    file.delete();
                }
            }
        } else {
            System.out.println(M + "No files found in snapshot directory");
        }

        System.out.println(M + "Resetting WAL log...");

        Path logFpath = store.getLogHandler().getLogger().getPath();
        System.out.println(M + "WAL path: " + logFpath);

        Files.delete(logFpath);
        System.out.println(M + "Deleted old WAL");

        Files.createFile(logFpath);
        System.out.println(M + "Created new WAL file");

        store.getLogHandler().setLogger(new WALogger(logFpath));
        System.out.println(M + "WAL logger reset complete");

        System.out.println(M + "Creating AppendEntryResponse");

        ByteBuffer response = ByteBuffer.wrap(
                new AppendEntryResponse(lastLogId, lastTerm, true).serialize()
        );

        System.out.println(M + "completed successfully");

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

    private void cleanup(SelectionKey key) throws IOException {
        if (!key.isValid()) return;

        SelectableChannel channel = key.channel();
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
}
