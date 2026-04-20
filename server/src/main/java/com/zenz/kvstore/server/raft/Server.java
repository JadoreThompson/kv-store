package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
import com.zenz.kvstore.server.restore.RaftKVStoreRestorer;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

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
    private ServerSocketChannel socketChannel;
    private final Map<SocketChannel, Queue<ByteBuffer>> pendingWrites = new HashMap<>();
    @Getter
    private volatile boolean isRunning;

    private long prevLogTerm = -1;
    private long prevLogId = -1;
    private int prevLogIndex = -1;
    private Path snapshotPath;
    private FileChannel fileChannel;

    @Getter
    private long lastAppendEntryTs;

    @Getter
    @Setter
    private ServerObserver observer;

    private Logger log;

    public Server(final InetSocketAddress address) {
        this.address = address;
        log = LoggerFactory.getLogger(this.getClass().getName());
    }

    public void open() throws IOException {
        if (isRunning) {
            return;
        }

        isRunning = true;
        selector = Selector.open();
        socketChannel = ServerSocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.bind(address);
        socketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (isRunning) {
            final int readyCount = selector.select();

            if (readyCount == 0) {
                continue;
            }

            final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                final SelectionKey key = it.next();
                it.remove();

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

        log.info("Existing loop");
    }

    @Override
    public void close() throws IOException {
        log.info("Close was called");
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
                buffer.put(session.buffer.array());
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
            observer.onReceive(message);
        } catch (BufferUnderflowException | InvalidMessageException e) {
            return false;
        }


        Message responseMessage = null;
        switch (message.type()) {
            case REQUEST_VOTE -> {
                responseMessage = handleRequestVote((RequestVote) message);
            }
            case APPEND_ENTRY -> {
                responseMessage = handleAppendEntry((AppendEntry) message);
            }
            case INSTALL_SNAPSHOT -> {
                responseMessage = handleInstallSnapshot((InstallSnapshot) message);
            }
        }

        if (responseMessage != null) {
            observer.onSend(responseMessage);
            queueWrite(session.channel, ByteBuffer.wrap(responseMessage.serialize()));
        }

        return true;
    }

    private Message handleRequestVote(final RequestVote requestVote) {
        final long currentTerm = stateObject.getCurrentTerm();
        stateObject.setCurrentTerm(requestVote.term());
        if (
                requestVote.term() <= currentTerm ||
                        requestVote.term() <= stateObject.votedTerm ||
                        requestVote.lastLogId() < logHandler.getLogId() ||
                        (requestVote.lastLogId() == logHandler.getLogId()
                                && requestVote.lastLogTerm() < logHandler.getTerm())) {
            return new RequestVoteResponse(requestVote.term(), false);
        }

        stateObject.votedTerm = requestVote.term();
        return new RequestVoteResponse(requestVote.term(), true);
    }

    Message handleAppendEntry(final AppendEntry appendEntry) {
        if (appendEntry.term() < stateObject.getCurrentTerm()) {
            return new AppendEntryResponse(
                    stateObject.getCurrentTerm(),
                    AppendEntryResponse.FailureReason.GREATER_TERM,
                    prevLogId,
                    prevLogTerm,
                    -1,
                    -1);
        }

        lastAppendEntryTs = System.currentTimeMillis();
        stateObject.setCurrentTerm(appendEntry.term());

        if (stateObject.getLeaderId() == null || !stateObject.getLeaderId().equals(appendEntry.leaderId())) {
            manager.setLeader(appendEntry.leaderId());
            prevLogId = logHandler.getSeedEntry().id;
            prevLogTerm = logHandler.getSeedEntry().term;
        }

        long prevLogId = this.prevLogId;
        long prevLogTerm = this.prevLogTerm;

        if (appendEntry.prevLogId() != prevLogId || appendEntry.prevLogTerm() != prevLogTerm) {
            return new AppendEntryResponse(
                    stateObject.getCurrentTerm(),
                    AppendEntryResponse.FailureReason.PREV_LOG_MISMATCH,
                    prevLogId,
                    prevLogTerm,
                    -1,
                    -1);
        }

        if (prevLogIndex >= logHandler.getEntries().size()) {
            prevLogIndex = -1;
        }

        for (RaftLogEntry entry : appendEntry.entries()) {
            ++prevLogIndex;
            if (!logHandler.getEntries().isEmpty() && prevLogIndex < logHandler.getEntries().size()) {
                final RaftLogEntry existingEntry = logHandler.getEntries().get(prevLogIndex);
                if (existingEntry.id != entry.id) {
                    logHandler.setEntries(logHandler.getEntries().subList(0, prevLogIndex));
                }
            }
            logHandler.getEntries().add(entry);
        }

        if (!appendEntry.entries().isEmpty()) {
            logHandler.setLogId(appendEntry.entries().getLast().id);
            logHandler.setTerm(appendEntry.entries().getLast().term);
        }

        this.prevLogId = logHandler.getLogId();
        this.prevLogTerm = logHandler.getTerm();
        return new AppendEntryResponse(
                stateObject.getCurrentTerm(),
                null,
                prevLogId,
                prevLogTerm,
                this.prevLogId,
                this.prevLogTerm);
    }

    Message handleInstallSnapshot(final InstallSnapshot installSnapshot) throws IOException {
        final long currentTerm = stateObject.getCurrentTerm();

        if (installSnapshot.term() < currentTerm) {
            return new InstallSnapshotResponse(currentTerm);
        }

        stateObject.setCurrentTerm(installSnapshot.term());
        if (stateObject.getLeaderId() == null || !stateObject.getLeaderId().equals(installSnapshot.leaderId())) {
            manager.setLeader(installSnapshot.leaderId());
        }

        if (installSnapshot.offset() == 0) {
            if (snapshotPath != null) {
                closeFileChannel();
                if (!snapshotPath.toFile().delete()) {
                    throw new IOException("Unable to delete snapshot file");
                }
            }
            snapshotPath = logHandler.getSnapshotter().getFpath(List.of(new RaftLogEntry(
                    installSnapshot.lastIncludedId(),
                    installSnapshot.lastIncludedTerm(),
                    null)));
            fileChannel = FileChannel.open(snapshotPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }

        fileChannel.write(ByteBuffer.wrap(installSnapshot.data()), installSnapshot.offset());

        if (installSnapshot.done()) {
            fileChannel.force(true);
            closeFileChannel();

            logHandler = new RaftLogHandler(logHandler.getLogger(), logHandler.getSnapshotter());
            logHandler.setLogId(installSnapshot.lastIncludedId());
            logHandler.setTerm(installSnapshot.lastIncludedTerm());

            final RaftKVStoreRestorer restorer = new RaftKVStoreRestorer();
            final KVStore newKvstore = restorer.restore(new KVStore(logHandler));
            manager.setKvstore(newKvstore);
        }

        return new InstallSnapshotResponse(currentTerm);
    }

    private void closeFileChannel() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
    }

    private void queueWrite(final SocketChannel channel, final ByteBuffer buffer) {
        final Queue<ByteBuffer> queue = pendingWrites.computeIfAbsent(channel, k -> new LinkedList<>());
        queue.offer(buffer.duplicate());

        final SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
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
        log = LoggerFactory.getLogger(String.format("[%s][Server] ", manager.getNodeConfig().id()));
    }

    private static class Session {

        public final SocketChannel channel;
        public ByteBuffer buffer = ByteBuffer.allocate(1024);

        public Session(final SocketChannel channel) {
            this.channel = channel;
        }
    }
}
