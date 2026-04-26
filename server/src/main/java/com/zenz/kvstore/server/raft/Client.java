package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
import com.zenz.kvstore.server.snapshot.RaftSnapshotBody;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.*;

public class Client implements Closeable {

    private static final int BATCH_SIZE = 10;

    @Getter
    @Setter
    private int batchSize = BATCH_SIZE;

    @Getter
    private final InetSocketAddress remoteAddress;

    @Getter
    public Manager manager;

    @Getter
    @Setter
    private volatile StateObject stateObject;

    @Getter
    public long lastMessageTs;

    @Getter
    private volatile boolean isOpen;

    @Getter
    private volatile boolean isConnected;

    private Selector selector;
    private SocketChannel socketChannel;
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private final Queue<ByteBuffer> pendingWrites = new LinkedList<>();

    private RaftLogHandler logHandler;
    private long electionTerm;

    private int prevLogIndex = -1;
    private long prevLogId = -1;
    private long prevLogTerm = -1;
    private SnapshotContext snapshotContext;

    @Getter
    @Setter
    private ClientObserver observer;

    private String DEBUG_PREFIX = "";
    private Logger log;

    public Client(final InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
        log = LoggerFactory.getLogger(this.getClass());
    }

    public void open() throws IOException {
        if (isOpen) {
            return;
        }

        isOpen = true;
        openSocketChannel();

        while (isOpen) {
            final State curState = stateObject.state;
            switch (curState) {
                case CANDIDATE -> handleCandidateState();
                case LEADER -> handleLeaderState();
            }

            final int readyCount = selector.select(10);
            if (readyCount == 0) {
                continue;
            }

            final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                final SelectionKey key = it.next();
                it.remove();

                try {
                    if (key.isConnectable()) {
                        handleConnect(key);
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
        selector = Selector.open();
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(remoteAddress);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
    }

    private void handleConnect(final SelectionKey key) throws IOException {
        final SocketChannel channel = (SocketChannel) key.channel();
        if (isConnected) {
            return;
        }

        try {
            if (channel.finishConnect()) {
                isConnected = true;
                key.interestOps(SelectionKey.OP_READ);
                return;
            }
        } catch (IOException _) {
        }
        closeSocket();
        openSocketChannel();
    }

    private void handleRead(final SelectionKey key) throws IOException {
        final SocketChannel client = (SocketChannel) key.channel();

        int totalRead = 0;
        int readCount = 0;
        do {
            if (readBuffer.position() == readBuffer.limit()) {
                final ByteBuffer buffer = ByteBuffer.allocate(readBuffer.capacity() * 2);
                buffer.put(readBuffer.array());
                buffer.flip();
                buffer.limit(buffer.capacity());
                buffer.position(readBuffer.position());
                readBuffer = buffer;
            }

            readCount = client.read(readBuffer);
            totalRead += readCount;

            if (readCount == -1) {
                close();
                return;
            }
        } while (readCount > 0);

        if (totalRead == 0) {
            return;
        }

        final int prevPosition = readBuffer.position();
        readBuffer.flip();
        final boolean processed = processData();

        if (processed) {
            readBuffer.clear();
            readBuffer = ByteBuffer.allocate(1024);
        } else {
            readBuffer.flip();
            readBuffer.limit(readBuffer.capacity());
            readBuffer.position(prevPosition);
        }
    }

    private boolean processData() throws IOException {
        Message message;
        try {
            message = Message.deserialize(readBuffer);
            observer.onReceive(remoteAddress, message);
        } catch (BufferUnderflowException | InvalidMessageException e) {
            return false;
        }

        Message responseMessage = null;
        switch (message.type()) {
            case REQUEST_VOTE_RESPONSE -> {
                manager.handleRequestVoteResponse((RequestVoteResponse) message);
            }
            case APPEND_ENTRY_RESPONSE -> {
                responseMessage = handleAppendEntryResponse((AppendEntryResponse) message);
            }
            case INSTALL_SNAPSHOT -> {
                responseMessage = handleInstallSnapshotResponse((InstallSnapshotResponse) message);
            }
        }

        if (responseMessage != null) {
            observer.onSend(remoteAddress, responseMessage);
            queueWrite(ByteBuffer.wrap(responseMessage.serialize()));
        }

        return true;
    }

    private void queueWrite(final ByteBuffer buffer) {
        pendingWrites.offer(buffer.duplicate());
        final SelectionKey key = socketChannel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    Message handleAppendEntryResponse(final AppendEntryResponse response) throws IOException {
        if (response.isSuccess()) {
            return createAppendEntryRequest();
        }

        switch (response.failureReason()) {
            case GREATER_TERM -> stateObject.setState(State.FOLLOWER);
            case PREV_LOG_MISMATCH -> {
                final Path snapshotPath = logHandler.getSnapshotter().findSnapshot(response.prevLogId());
                final RaftSnapshotBody body = logHandler.getSnapshotter().getBody(snapshotPath);
                snapshotContext = new SnapshotContext(snapshotPath, body.getEntries());
                return createInstallSnapshot();
            }
        }

        return null;
    }

    Message handleInstallSnapshotResponse(final InstallSnapshotResponse response) {
        if (snapshotContext == null) {
            throw new RaftClientException("Snapshot context is not set");
        }

        if (response.term() > stateObject.getCurrentTerm()) {
            stateObject.setState(State.FOLLOWER);
            return null;
        }

        if (snapshotContext.index >= snapshotContext.entries.size()) {
            snapshotContext = null;
            return createAppendEntryRequest();
        }

        return createInstallSnapshot();
    }

    private void handleCandidateState() {
        StateObject.Election election = stateObject.election;
        if (election == null || election.isDone()) {
            return;
        }

        if (election.isExpired()) {
            election = manager.startElection();
        }

        final long electionTerm = election.term;
        if (this.electionTerm != electionTerm && !election.isDone()) {
            try {
                queueWrite(ByteBuffer.wrap(new RequestVote(
                        manager.getNodeConfig().id(),
                        electionTerm,
                        logHandler.getLogId(),
                        logHandler.getTerm()).serialize()));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-2);
            }
            this.electionTerm = election.term;
        }
    }

    private void handleLeaderState() {
        if (prevLogId != -1 && prevLogId == logHandler.getLogId()) {
            if (System.currentTimeMillis() - lastMessageTs > 100) {
                queueWrite(ByteBuffer.wrap(new AppendEntry(
                        manager.getNodeConfig().id(),
                        stateObject.getCurrentTerm(),
                        prevLogId,
                        prevLogTerm,
                        Collections.emptyList()).serialize()));
                lastMessageTs = System.currentTimeMillis();
            }
        } else {
            try {
                queueWrite(ByteBuffer.wrap(createAppendEntryRequest().serialize()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    AppendEntry createAppendEntryRequest() {
        if (prevLogId != -1 && prevLogId == logHandler.getLogId()) {
            return new AppendEntry(
                    manager.getNodeConfig().id(),
                    stateObject.getCurrentTerm(),
                    prevLogId,
                    prevLogTerm,
                    Collections.emptyList());
        }

        if (prevLogId == -1 && prevLogTerm == -1) {
            prevLogId = logHandler.getSeedEntry().id;
            prevLogTerm = logHandler.getSeedEntry().term;
        }

        // We suspect a snapshot occurred
        if (
                prevLogIndex >= 0 &&
                        (prevLogIndex >= logHandler.getEntries().size() ||
                                logHandler.getEntries().get(prevLogIndex).id != prevLogId)) {
            prevLogIndex = -1;
            final RaftLogEntry seedLogEntry = logHandler.getSeedEntry();
            prevLogId = seedLogEntry.id;
            prevLogTerm = seedLogEntry.term;
        }

        final int nextLogIndex = prevLogIndex + 1;
        long prevLogId;
        long prevLogTerm;
        List<RaftLogEntry> entries;
        if (logHandler.getEntries().isEmpty()) {
            final RaftLogEntry seed = logHandler.getSeedEntry();
            prevLogId = seed.id;
            prevLogTerm = seed.term;
            entries = Collections.emptyList();
        } else {
            entries = logHandler.getEntries().subList(
                    nextLogIndex,
                    Math.min(logHandler.getEntries().size(), nextLogIndex + batchSize));
            prevLogId = this.prevLogId;
            prevLogTerm = this.prevLogTerm;
            prevLogIndex = prevLogIndex + entries.size();
            this.prevLogId = entries.getLast().id;
            this.prevLogTerm = entries.getLast().term;
        }

        return new AppendEntry(
                manager.getNodeConfig().id(),
                stateObject.getCurrentTerm(),
                prevLogId,
                prevLogTerm,
                entries);
    }

    InstallSnapshot createInstallSnapshot() {
        if (snapshotContext == null) {
            throw new RuntimeException("Snapshot context must be set before creating InstallSnapshot");
        }

        final List<RaftLogEntry> entries = snapshotContext.entries.subList(
                snapshotContext.index,
                Math.min(snapshotContext.entries.size(), snapshotContext.index + batchSize));
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        for (RaftLogEntry entry : entries) {
            final byte[] entryBytes = entry.serialize();
            if (buffer.position() + entryBytes.length > buffer.capacity()) {
                final ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() + entryBytes.length);
                newBuffer.put(buffer);
                newBuffer.flip();
                newBuffer.position(buffer.position());
                newBuffer.limit(newBuffer.capacity());
                buffer = newBuffer;
            }
            buffer.put(entryBytes);
        }

        snapshotContext.index += batchSize;
        snapshotContext.offset += buffer.capacity();

        return new InstallSnapshot(
                manager.getNodeConfig().id(),
                stateObject.getCurrentTerm(),
                entries.getLast().id,
                entries.getLast().term,
                snapshotContext.offset,
                buffer.array(),
                snapshotContext.index >= snapshotContext.entries.size()
        );
    }

    private void handleWrite(final SelectionKey key) throws IOException {
        if (pendingWrites.isEmpty()) {
            return;
        }

        final SocketChannel client = (SocketChannel) key.channel();
        final ByteBuffer buffer = pendingWrites.peek();
        while (buffer.hasRemaining()) {
            final int readCount = client.write(buffer);
            if (readCount == 0) {
                break;
            }
        }

        if (!buffer.hasRemaining()) {
            pendingWrites.poll();
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    public void close() throws IOException {
        if (!isOpen) {
            return;
        }

        isOpen = false;
        closeSocket();
        isConnected = false;
        manager.handleClientClose(this);
    }

    private void closeSocket() throws IOException {
        socketChannel.close();
        selector.close();
    }

    public void setManager(final Manager manager) {
        this.manager = manager;
        logHandler = (RaftLogHandler) manager.getKvstore().getLogHandler();
        DEBUG_PREFIX = String.format("[%s][Client]", manager.getNodeConfig().id());
        log = LoggerFactory.getLogger(DEBUG_PREFIX);
    }

    private static class SnapshotContext {

        public final Path snapshotPath;
        public final List<RaftLogEntry> entries;
        public int index;
        public int offset;

        public SnapshotContext(final Path snapshotPath, final List<RaftLogEntry> entries) {
            this.snapshotPath = snapshotPath;
            this.entries = entries;
        }
    }

    private static class RaftClientException extends RuntimeException {

        public RaftClientException(final String message) {
            super(message);
        }
    }
}
