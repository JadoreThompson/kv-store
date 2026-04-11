package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Client implements Closeable {

    @Getter
    private final InetSocketAddress remoteAddress;

    @Getter
    @Setter
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
    private long lastSentMessage;
    private int prevLogIndex;
    private RaftLogEntry prevLogEntry;

    public Client(final InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public void open() throws IOException {
        if (isOpen) {
            return;
        }

        isOpen = true;
        selector = Selector.open();
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(remoteAddress);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);

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

    private ByteBuffer handleAppendEntryResponse(final AppendEntryResponse response) {
        return null;
    }

    private ByteBuffer handleInstallSnapshotResponse(final InstallSnapshotResponse response) {
        return null;
    }

    private void handleCandidateState() {
        StateObject.Election election = stateObject.election;

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
        if (System.currentTimeMillis() - lastMessageTs > 100) {
            queueWrite(ByteBuffer.wrap(new AppendEntry(
                    manager.getNodeConfig().id(),
                    stateObject.getCurrentTerm(),
                    logHandler.getLastEntry().id,
                    logHandler.getLastEntry().term,
                    Collections.emptyList()).serialize()));
            lastMessageTs = System.currentTimeMillis();
        }
    }

    private void sendAppendEntry() {
        // We suspect a snapshot occurred
        if (prevLogIndex >= logHandler.getEntries().size() || logHandler.getEntries().get(prevLogIndex) != prevLogEntry) {
            prevLogIndex = -1;
            prevLogEntry = logHandler.getSeedEntry();
        }

        final int batchSize = 10;
        final int nextLogIndex = prevLogIndex + 1;
        final List<RaftLogEntry> entries = logHandler.getEntries().subList(nextLogIndex, nextLogIndex + batchSize);
        final RaftLogEntry prevLogEntry = this.prevLogEntry;
        this.prevLogEntry = entries.getLast();
        prevLogIndex = prevLogIndex + entries.size();

        queueWrite(ByteBuffer.wrap(new AppendEntry(
                manager.getNodeConfig().id(),
                stateObject.getCurrentTerm(),
                prevLogEntry.id,
                prevLogEntry.term,
                entries).serialize()));
    }

    private void sendInstallSnapshot() {
    }

    private void handleWrite(final SelectionKey key) throws IOException {
    }

    public void close() {
        if (!isOpen) {
            return;
        }

        isOpen = false;
        isConnected = false;
    }

    public void setManager(final Manager manager) {
        this.manager = manager;
        logHandler = (RaftLogHandler) manager.getKvstore().getLogHandler();
    }
}
