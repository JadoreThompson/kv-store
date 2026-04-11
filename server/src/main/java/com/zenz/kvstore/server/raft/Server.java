package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

@Slf4j
public class Server implements AutoCloseable {

    @Getter
    private final InetSocketAddress address;

    @Getter
    public Manager manager;

    private RaftLogHandler logHandler;

    @Getter
    @Setter
    public StateObject stateObject;

    private Selector selector;
    private SocketChannel socketChannel;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
    volatile private boolean isRunning;

    private long prevLogTerm = -1;
    private long prevLogId = -1;
    private Path snapshotPath;
    private FileChannel fileChannel;

    public Server(final InetSocketAddress address) {
        this.address = address;
    }

    public void open() throws Exception {
        if (isRunning) {
            return;
        }

        isRunning = true;
        selector = Selector.open();
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.bind(address);
        socketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (isRunning) {
            final int readyCount = selector.select();

            if (readyCount == 0) {
                continue;
            }

            for (SelectionKey key : selector.keys()) {
                try {
                    if (key.isAcceptable()) {
                        handleAccept(key);
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

    @Override
    public void close() throws Exception {
        if (!isRunning) {
            return;
        }

        isRunning = false;
        if (selector != null) {
            for (SelectionKey key : selector.keys()) {
                cleanup(key);
            }
            selector.close();
        }
        if (socketChannel != null) {
            socketChannel.close();
        }

    }

    private void handleAccept(final SelectionKey key) throws IOException {
        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel client = server.accept();

        if (client == null) {
            return;
        }

        client.configureBlocking(false);
        client.socket().setTcpNoDelay(true);
        client.socket().setKeepAlive(true);

        SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
        clientKey.attach(new Session(client));
    }

    private void handleRead(final SelectionKey key) throws IOException {
        final SocketChannel client = (SocketChannel) key.channel();
        final Session session = (Session) key.attachment();

        int totalRead = 0;
        int readCount = 0;
        do {
            if (session.buffer.position() == session.buffer.limit()) {
                final ByteBuffer buffer = ByteBuffer.allocate(session.buffer.capacity() * 2);
                buffer.put(session.buffer);
                buffer.flip();
                buffer.limit(buffer.capacity());
                buffer.position(session.buffer.position());
                session.buffer = buffer;
            }

            readCount = client.read(session.buffer);
            totalRead += readCount;

            if (readCount == -1) {
                cleanup(key);
            }
        } while (readCount > 0);

        if (totalRead == 0) {
            return;
        }

        final int prevPosition = session.buffer.position();
        session.buffer.flip();
        final boolean processed = processData(session);

        if (processed) {
            session.buffer.clear();
            session.buffer = ByteBuffer.allocate(1024);
        } else {
            session.buffer.flip();
            session.buffer.position(prevPosition);
            session.buffer.limit(session.buffer.capacity());
        }
    }

    private void cleanup(final SelectionKey key) throws IOException {
        if (key == null || !key.isValid()) {
            return;
        }

        final SelectableChannel client = key.channel();
        pendingWrites.remove(client);
        key.cancel();

        try {
            client.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    private boolean processData(final Session session) throws IOException {
        Message message;
        try {
            message = Message.deserialize(session.buffer);
        } catch (BufferUnderflowException | InvalidMessageException e) {
            return false;
        }

        ByteBuffer responseBuffer = null;
        switch (message.type()) {
            case REQUEST_VOTE -> {
                responseBuffer = handleRequestVote((RequestVote) message);
            }
            case APPEND_ENTRY -> {
                responseBuffer = handleAppendEntry((AppendEntry) message);
            }
            case INSTALL_SNAPSHOT -> {
                responseBuffer = handleInstallSnapshot((InstallSnapshot) message);
            }
        }

        if (responseBuffer != null) {
            queueWrite(session.channel, responseBuffer);
        }

        return true;
    }

    private ByteBuffer handleRequestVote(final RequestVote requestVote) {
        stateObject.setCurrentTerm(requestVote.term());

        if (
                requestVote.term() <= stateObject.getCurrentTerm() ||
                        requestVote.term() <= stateObject.votedTerm ||
                        requestVote.lastLogId() < logHandler.getLogId() ||
                        requestVote.lastLogId() == logHandler.getLogId() && requestVote.lastLogTerm() != logHandler.getTerm()) {
            return ByteBuffer.wrap(new RequestVoteResponse(requestVote.term(), false).serialize());
        }

        stateObject.votedTerm = requestVote.term();
        return ByteBuffer.wrap(new RequestVoteResponse(requestVote.term(), true).serialize());
    }

    private ByteBuffer handleAppendEntry(final AppendEntry appendEntry) throws IOException {
        long prevLogId = this.prevLogId;
        long prevLogTerm = this.prevLogTerm;

        if (prevLogId == -1 || prevLogTerm == -1) {
            final RaftLogEntry firstEntry = logHandler.getFirstEntry();
            if (firstEntry != null) {
                prevLogId = firstEntry.id;
                prevLogTerm = firstEntry.term;
            } else {
                final RaftLogEntry seedEntry = logHandler.getSeedEntry();
                prevLogId = seedEntry.id;
                prevLogTerm = seedEntry.term;
            }
        }

        if (appendEntry.term() < stateObject.getCurrentTerm()) {
            return ByteBuffer.wrap(new AppendEntryResponse(
                    stateObject.getCurrentTerm(),
                    AppendEntryResponse.FailureReason.TERM,
                    prevLogId,
                    prevLogTerm,
                    -1,
                    -1).serialize());
        }

        stateObject.setCurrentTerm(appendEntry.term());

        if (appendEntry.prevLogId() != prevLogId || appendEntry.prevLogTerm() != prevLogTerm) {
            return ByteBuffer.wrap(new AppendEntryResponse(
                    stateObject.getCurrentTerm(),
                    AppendEntryResponse.FailureReason.PREV_LOG,
                    prevLogId,
                    prevLogTerm,
                    -1,
                    -1).serialize());
        }

        for (RaftLogEntry entry : logHandler.getEntries()) {
            final Command command = entry.command;
            switch (command.type()) {
                case PUT -> {
                    final PutCommand comm = (PutCommand) command;
                    manager.getKvstore().put(comm.key(), comm.value());
                }
                case DELETE -> {
                    final DeleteCommand comm = (DeleteCommand) command;
                    manager.getKvstore().delete(comm.key());
                }
            }
        }

        this.prevLogId = logHandler.getLastEntry().id;
        this.prevLogTerm = logHandler.getLastEntry().term;
        return ByteBuffer.wrap(new AppendEntryResponse(
                stateObject.getCurrentTerm(),
                null,
                prevLogId,
                prevLogTerm,
                logHandler.getEntries().getLast().id,
                logHandler.getEntries().getLast().term).serialize());
    }

    private ByteBuffer handleInstallSnapshot(final InstallSnapshot installSnapshot) throws IOException {
        final long currentTerm = stateObject.getCurrentTerm();

        if (installSnapshot.term() < currentTerm) {
            return ByteBuffer.wrap(new InstallSnapshotResponse(currentTerm).serialize());
        }

        stateObject.setCurrentTerm(installSnapshot.term());

        if (installSnapshot.offset() == 0) {
            closeFileChannel();
            if (!snapshotPath.toFile().delete()) {
                throw new IOException("Unable to delete snapshot file");
            }
            snapshotPath = logHandler.getSnapshotter().getFpath(List.of(new RaftLogEntry(
                    installSnapshot.lastIncludedId(), installSnapshot.lastIncludedTerm(), null)));
            fileChannel = FileChannel.open(snapshotPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }

        fileChannel.write(
                new ByteBuffer[]{ByteBuffer.wrap(installSnapshot.data())},
                installSnapshot.offset(),
                installSnapshot.data().length);

        if (installSnapshot.done()) {
            fileChannel.force(true);
            closeFileChannel();

            logHandler = new RaftLogHandler(logHandler.getLogger(), logHandler.getSnapshotter());
            logHandler.setLogId(installSnapshot.lastIncludedId() - 1);
            logHandler.setTerm(installSnapshot.lastIncludedTerm());
            final KVStore newKvstore = new KVStore(logHandler);
            logHandler.getSnapshotter().restore(newKvstore);
            manager.setKvstore(newKvstore);
        }

        return ByteBuffer.wrap(new InstallSnapshotResponse(currentTerm).serialize());
    }

    private void closeFileChannel() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
    }

    private void queueWrite(final SocketChannel channel, final ByteBuffer buffer) {
        final Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        if (queue.isEmpty()) {
            return;
        }
        queue.offer(buffer.duplicate());

        final SelectionKey key = channel.keyFor(selector);
        if (key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void handleWrite(final SelectionKey key) throws IOException {
        final Session session = (Session) key.attachment();
        if (session == null) {
            return;
        }

        final Queue<ByteBuffer> queue = pendingWrites.get(session.channel);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        final ByteBuffer buffer = queue.peek();
        while (buffer.hasRemaining()) {
            final int writeCount = session.channel.write(buffer);
            if (writeCount == 0) {
                break;
            }
        }

        if (!buffer.hasRemaining()) {
            queue.poll();
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    public void setManager(final Manager manager) {
        this.manager = manager;
        logHandler = (RaftLogHandler) manager.getKvstore().getLogHandler();
    }

    private static class Session {

        public final SocketChannel channel;
        public ByteBuffer buffer = ByteBuffer.allocate(1024);

        public Session(final SocketChannel channel) {
            this.channel = channel;
        }
    }
}
