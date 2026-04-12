package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
import com.zenz.kvstore.server.snapshot.RaftSnapshotBody;
import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Client implements Closeable {

    private static final int BATCH_SIZE = 10;

    @Getter
    private final InetSocketAddress remoteAddress;

    @Getter
    public Manager manager;

    @Getter
    @Setter
    private StateObject stateObject;

    @Getter
    public long lastMessageTs;

    @Getter
    private boolean isOpen;

    @Getter
    private boolean isConnected;

    private Selector selector;
    private SocketChannel socketChannel;
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private final Queue<ByteBuffer> pendingWrites = new LinkedList<>();

    private RaftLogHandler logHandler;
    private long electionTerm;

    private int prevLogIndex = -1;
    private RaftLogEntry prevLogEntry;
    private SnapshotContext snapshotContext;

    public Client(final InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
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

            final int readyCount = selector.select(100);
            if (readyCount == 0) {
                continue;
            }

            for (SelectionKey key : selector.keys()) {
                if (!key.isValid()) {
                    continue;
                }

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
        isConnected = true;
    }

    private void handleRead(final SelectionKey key) throws IOException {
        final SocketChannel client = (SocketChannel) key.channel();

        int totalRead = 0;
        int readCount = 0;
        do {
            if (readBuffer.position() == readBuffer.limit()) {
                final ByteBuffer buffer = ByteBuffer.allocate(readBuffer.capacity() * 2);
                buffer.put(readBuffer);
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
            readBuffer.position(prevPosition);
            readBuffer.limit(readBuffer.capacity());
        }
    }

    private boolean processData() throws IOException {
        Message message;
        try {
            message = Message.deserialize(readBuffer);
        } catch (BufferUnderflowException | InvalidMessageException e) {
            return false;
        }

        ByteBuffer responseBuffer = null;
        switch (message.type()) {
            case REQUEST_VOTE_RESPONSE -> {
                manager.handleRequestVoteResponse((RequestVoteResponse) message);
            }
            case APPEND_ENTRY_RESPONSE -> {
                responseBuffer = handleAppendEntryResponse((AppendEntryResponse) message);
            }
            case INSTALL_SNAPSHOT -> {
                responseBuffer = handleInstallSnapshotResponse((InstallSnapshotResponse) message);
            }
        }

        if (responseBuffer != null) {
            queueWrite(responseBuffer);
        }

        return true;
    }

    private void queueWrite(final ByteBuffer buffer) {
        if (pendingWrites.isEmpty()) {
            return;
        }

        pendingWrites.offer(buffer.duplicate());

        final SelectionKey key = socketChannel.keyFor(selector);
        if (key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private ByteBuffer handleAppendEntryResponse(final AppendEntryResponse response) throws IOException {
        if (response.isSuccess()) {
            return ByteBuffer.wrap(createAppendEntryRequest().serialize());
        }

        switch (response.failureReason()) {
            case TERM -> stateObject.state = State.FOLLOWER;
            case PREV_LOG -> {
                if (prevLogIndex - BATCH_SIZE < 0) {
                    final Path snapshotPath = logHandler.getSnapshotter().findSnapshot(response.prevLogId());
                    final RaftSnapshotBody body = logHandler.getSnapshotter().getBody(snapshotPath);
                    snapshotContext = new SnapshotContext(snapshotPath, body.getEntries());
                    final InstallSnapshot installSnapshot = createInstallSnapshot();
                    return ByteBuffer.wrap(installSnapshot.serialize());
                }
            }
        }

        return null;
    }

    private ByteBuffer handleInstallSnapshotResponse(final InstallSnapshotResponse response) throws IOException {
        if (response.term() > stateObject.getCurrentTerm()) {
            stateObject.state = State.FOLLOWER;
            return null;
        }

        if (snapshotContext == null) {
            return null;
        }

        if (snapshotContext.index >= snapshotContext.entries.size()) {
            snapshotContext = null;
            return ByteBuffer.wrap(createAppendEntryRequest().serialize());
        }

        return ByteBuffer.wrap(createInstallSnapshot().serialize());
    }

    private void handleCandidateState() {
        StateObject.Election election = stateObject.election;
        if (election == null || election.isDone()) {
            return;
        }

        if (election.isExpired()) {
            election = manager.startElection();
        }

        if (electionTerm != election.term && !election.isDone()) {
            queueWrite(ByteBuffer.wrap(new RequestVote(
                    manager.getNodeConfig().id(),
                    election.term,
                    logHandler.getLastEntry().getId(),
                    logHandler.getLastEntry().getTerm()).serialize()));
            electionTerm = election.term;
        }
    }

    private void handleLeaderState() {
        if (prevLogEntry.id == logHandler.getLogId()) {
            if (System.currentTimeMillis() - lastMessageTs > 100) {
                queueWrite(ByteBuffer.wrap(new AppendEntry(
                        manager.getNodeConfig().id(),
                        stateObject.getCurrentTerm(),
                        logHandler.getLastEntry().id,
                        logHandler.getLastEntry().term,
                        Collections.emptyList()).serialize()));
                lastMessageTs = System.currentTimeMillis();
            }
        } else {
            queueWrite(ByteBuffer.wrap(createAppendEntryRequest().serialize()));
        }
    }

    private AppendEntry createAppendEntryRequest() {
        // We suspect a snapshot occurred
        if (
                prevLogIndex >= logHandler.getEntries().size() ||
                        logHandler.getEntries().get(prevLogIndex) != prevLogEntry) {
            prevLogIndex = -1;
            prevLogEntry = logHandler.getSeedEntry();
        }

        final int nextLogIndex = prevLogIndex + 1;
        final List<RaftLogEntry> entries = logHandler.getEntries().subList(nextLogIndex, nextLogIndex + BATCH_SIZE);
        final RaftLogEntry prevLogEntry = this.prevLogEntry;
        this.prevLogEntry = entries.getLast();
        prevLogIndex = prevLogIndex + entries.size();

        return new AppendEntry(
                manager.getNodeConfig().id(),
                stateObject.getCurrentTerm(),
                prevLogEntry.id,
                prevLogEntry.term,
                entries);
    }

    private InstallSnapshot createInstallSnapshot() {
        if (snapshotContext == null) {
            throw new RuntimeException("Snapshot context must be set before creating InstallSnapshot");
        }

        final List<RaftLogEntry> entries =
                snapshotContext.entries.subList(snapshotContext.index, snapshotContext.index + BATCH_SIZE);
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

        snapshotContext.index += BATCH_SIZE;
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
        isConnected = false;
        socketChannel.close();
        selector.close();
        manager.handleClientClose(this);
    }

    public void setManager(final Manager manager) {
        this.manager = manager;
        logHandler = (RaftLogHandler) manager.getKvstore().getLogHandler();
    }

    private static class SnapshotContext {

        private final Path snapshotPath;
        private final List<RaftLogEntry> entries;
        private int index;
        private int offset;

        public SnapshotContext(final Path snapshotPath, final List<RaftLogEntry> entries) {
            this.snapshotPath = snapshotPath;
            this.entries = entries;
        }
    }
}
