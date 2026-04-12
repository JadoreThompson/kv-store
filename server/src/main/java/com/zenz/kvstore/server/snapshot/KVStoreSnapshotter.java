package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import lombok.Getter;
import lombok.Setter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

@Getter
public class KVStoreSnapshotter<H extends Snapshot.Header, B extends Snapshot.Body, F extends Snapshot.Footer> {

    public static final Path DEFAULT_DIR = Path.of("snapshots");

    @Setter
    private Path dir = DEFAULT_DIR;
    private final Class<H> headerClass;
    private final Class<B> bodyClass;
    private final Class<F> footerClass;

    public KVStoreSnapshotter(final Class<H> headerClass, final Class<B> bodyClass, final Class<F> footerClass) {
        this.headerClass = headerClass;
        this.bodyClass = bodyClass;
        this.footerClass = footerClass;
    }

    /**
     * Creates a snapshot file with the filename of <last log entry id>.snapshot and returns the path
     *
     * @param entries Log entries to snapshot
     * @return Path to the snapshot file
     */
    public Path snapshot(final List<? extends LogEntry> entries) throws IOException {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("entries cannot be null or empty");
        }

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        final H header = SnapshotRegistry.createHeader(headerClass, entries);
        final byte[] headerBytes = header.serialize();
        buffer = expandBuffer(buffer, buffer.position() + Integer.BYTES + headerBytes.length);
        buffer.putInt(headerBytes.length);
        buffer.put(headerBytes);

        final B body = SnapshotRegistry.createBody(bodyClass, entries);
        final byte[] bodyBytes = body.serialize();
        buffer = expandBuffer(buffer, buffer.position() + Integer.BYTES + bodyBytes.length);
        buffer.putInt(bodyBytes.length);
        buffer.put(bodyBytes);

        final F footer = SnapshotRegistry.createFooter(footerClass, entries);
        final byte[] footerBytes = footer.serialize();
        buffer = expandBuffer(buffer, buffer.position() + Integer.BYTES + footerBytes.length);
        buffer.putInt(footerBytes.length);
        buffer.put(footerBytes);

        final String prefix = String.valueOf(entries.getLast().id);
        final String suffix = ".snapshot";

        final Path tempPath = Files.createTempFile(dir, prefix + "-", suffix);
        final Path destPath = dir.resolve(prefix + suffix);

        try (FileChannel channel =
                     FileChannel.open(tempPath, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buffer.flip();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }

        Files.move(
                tempPath,
                destPath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );

        return destPath;
    }

    private ByteBuffer expandBuffer(final ByteBuffer buffer, final int newCapacity) {
        final ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
        newBuffer.put(buffer);
        return newBuffer;
    }

    public H getHeader(final Path snapshotPath) throws IOException {
        try (final InputStream is = new FileInputStream(snapshotPath.toString())) {
            final ByteBuffer buffer = ByteBuffer.wrap(is.readNBytes(4));
            final int len = buffer.getInt();
            return SnapshotRegistry.deserializeHeader(headerClass, ByteBuffer.wrap(is.readNBytes(len)));
        }
    }

    public B getBody(final Path snapshotPath) throws IOException {
        try (final InputStream is = new FileInputStream(snapshotPath.toString())) {
            // Skipping header
            ByteBuffer buffer = ByteBuffer.wrap(is.readNBytes(4));
            is.readNBytes(buffer.getInt());
            buffer.clear();

            // Fetching body
            buffer.put(is.readNBytes(4));
            final int len = buffer.getInt();
            return SnapshotRegistry.deserializeBody(bodyClass, ByteBuffer.wrap(is.readNBytes(len)));
        }
    }

    public F getFooter(final Path snapshotPath) throws IOException {
        try (final InputStream is = new FileInputStream(snapshotPath.toString())) {
            // Skipping header
            ByteBuffer buffer = ByteBuffer.wrap(is.readNBytes(4));
            is.readNBytes(buffer.getInt());
            buffer.clear();

            // Fetching body
            buffer.put(is.readNBytes(4));
            is.readNBytes(buffer.getInt());
            buffer.clear();

            buffer.put(is.readNBytes(4));
            final int len = buffer.getInt();
            return SnapshotRegistry.deserializeFooter(footerClass, ByteBuffer.wrap(is.readNBytes(len)));
        }
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

    public Path getFpath(final List<RaftLogEntry> entries) {
        final RaftLogEntry logEntry = entries.getLast();
        return dir.resolve(logEntry.id + ".snapshot");
    }
}
