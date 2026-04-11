package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.util.KVSerializable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class RaftKVMapSnapshotter implements Snapshotter<RaftLogEntry> {

    private final Path dir;

    public RaftKVMapSnapshotter(final Path dir) {
        this.dir = dir;
    }

    /**
     * Applies all snapshots to a KVStore object
     *
     * @param kvStore The KVStore instance to restore
     */
    public void restore(final KVStore kvStore) throws IOException {
        final File[] files = dir.toFile().listFiles();
        if (files == null) {
            return;
        }

        final RaftLogHandler logHandler = (RaftLogHandler) kvStore.getLogHandler();
        final Logger prevLogger = logHandler.getLogger();
        final Snapshotter<RaftLogEntry> prevSnapshotter = logHandler.getSnapshotter();
        final Logger logger = new Logger() {
            @Override
            public void log(LogEntry logEntry) {
            }
        };
        final Snapshotter<RaftLogEntry> snapshotter = new Snapshotter<RaftLogEntry>() {
            @Override
            public Path snapshot(List<RaftLogEntry> entries) {
                return null;
            }

            @Override
            public Path getFpath(List<RaftLogEntry> entries) {
                return null;
            }

            @Override
            public Path findSnapshot(long logId) {
                return null;
            }
        };
        logHandler.setLogger(logger);
        logHandler.setSnapshotter(snapshotter);

        for (File file : files) {
            try (final FileInputStream fis = new FileInputStream(file)) {
                ByteBuffer buffer = ByteBuffer.allocate(1024);

                // Fetching header
                buffer.put(fis.readNBytes(4));
                buffer.flip();
                final int headerSize = buffer.getInt();
                final Header header = Header.deserialize(ByteBuffer.wrap(fis.readNBytes(headerSize)));
                buffer.flip();
                buffer.clear();

                // Fetching body
                buffer.put(fis.readNBytes(4));
                buffer.flip();
                final int bodySize = buffer.getInt();
                final ByteBuffer bodyBuffer = ByteBuffer.wrap(fis.readNBytes(bodySize));

                // Applying records
                while (bodyBuffer.hasRemaining()) {
                    final RaftLogEntry logEntry = RaftLogEntry.deserialize(bodyBuffer);
                    switch (logEntry.command.type()) {
                        case PUT -> {
                            final PutCommand comm = (PutCommand) logEntry.command;
                            kvStore.put(comm.key(), comm.value());
                        }
                        case DELETE -> {
                            final DeleteCommand comm = (DeleteCommand) logEntry.command;
                            kvStore.delete(comm.key());
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        logHandler.setLogger(prevLogger);
        logHandler.setSnapshotter(prevSnapshotter);
    }

    /**
     * Creates a snapshot file with the filename of <last log entry id>.snapshot and returns the path
     *
     * @param entries Log entries to snapshot
     * @return Path to the snapshot file
     */
    public Path snapshot(final List<RaftLogEntry> entries) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        final ByteBuffer header = createHeader(entries);
        buffer.putInt(header.position());
        buffer = expandBuffer(buffer, buffer.capacity() + header.capacity());
        buffer.put(header);

        final ByteBuffer body = createBody(entries);
        buffer.putInt(body.position());
        buffer = expandBuffer(buffer, body.capacity() + body.capacity());
        buffer.put(body);

        final ByteBuffer footer = createFooter(entries);
        buffer.putInt(footer.position());
        buffer = expandBuffer(buffer, body.capacity() + footer.capacity());
        buffer.put(footer);

        final String prefix = "" + entries.getLast().id;
        final String suffix = ".snapshot";
        final Path destPath = dir.resolve(prefix + suffix);
        final Path tempPath = Files.createTempFile(prefix, suffix);
        try (final FileChannel channel = FileChannel.open(destPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            buffer.flip();
            channel.write(buffer);
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        tempPath.toFile().renameTo(destPath.toFile());
        return destPath;
    }

    private ByteBuffer expandBuffer(final ByteBuffer buffer, final int newCapacity) {
        final ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
        newBuffer.put(buffer);
        return newBuffer;
    }

    private ByteBuffer createHeader(final List<RaftLogEntry> entries) {
        return ByteBuffer.wrap(new Header(
                1,
                entries.getFirst().id,
                entries.getFirst().term,
                entries.getLast().id,
                entries.getFirst().term).serialize());
    }

    private ByteBuffer createBody(final List<RaftLogEntry> entries) {
        return ByteBuffer.wrap(new Body(entries).serialize());
    }

    private ByteBuffer createFooter(final List<RaftLogEntry> entries) {
        return ByteBuffer.wrap(new Footer(System.currentTimeMillis()).serialize());
    }

    /**
     * Finds the path to a snapshot file which will contain a log entry with the requested id
     *
     * @param logId
     * @return
     */
    public Path findSnapshot(final long logId) {
        return null;
    }

    @Override
    public Path getFpath(final List<RaftLogEntry> entries) {
        final RaftLogEntry logEntry = entries.getLast();
        return dir.resolve(logEntry.id + ".snapshot");
    }

    private Body deserializeBody(final ByteBuffer body) {
        final long numEntries = body.getLong();
        final List<RaftLogEntry> entries = new ArrayList<>();

        for (int i = 0; i < numEntries; i++) {
            entries.add(RaftLogEntry.deserialize(body));
        }

        return new Body(entries);
    }

    @Getter
    @RequiredArgsConstructor
    public static class Header implements KVSerializable {

        private final long version;
        private final long firstLogId;
        private final long firstLogTerm;
        private final long lastLogId;
        private final long lastLogTerm;

        @Override
        public byte[] serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(8 * 5);

            buffer.putLong(version);
            buffer.putLong(firstLogId);
            buffer.putLong(firstLogTerm);
            buffer.putLong(lastLogId);
            buffer.putLong(lastLogTerm);

            return buffer.array();
        }

        public static Header deserialize(final ByteBuffer buffer) {
            final long version = buffer.getLong();
            final long firstLogId = buffer.getLong();
            final long firstLogTerm = buffer.getLong();
            final long lastLogId = buffer.getLong();
            final long lastLogTerm = buffer.getLong();

            return new Header(
                    version,
                    firstLogId,
                    firstLogTerm,
                    lastLogId,
                    lastLogTerm
            );

        }
    }

    @Getter
    @RequiredArgsConstructor
    public static class Body implements KVSerializable {

        private final List<RaftLogEntry> entries;

        @Override
        public byte[] serialize() {
            final List<byte[]> serializedEntries = entries.stream().map(RaftLogEntry::serialize).toList();
            final ByteBuffer buffer = ByteBuffer.allocate(4 + serializedEntries.size());

            buffer.putInt(serializedEntries.size());
            for (byte[] serializedEntry : serializedEntries) {
                buffer.put(serializedEntry);
            }

            return buffer.array();
        }
    }

    @Getter
    @RequiredArgsConstructor
    public static class Footer implements KVSerializable {

        private final long timestamp;

        @Override
        public byte[] serialize() {
            final ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(timestamp);
            return buffer.array();
        }

        public static Footer deserialize(final ByteBuffer buffer) {
            final long timestamp = buffer.getLong();
            return new Footer(timestamp);
        }
    }
}
