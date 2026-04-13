package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
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
public class KVStoreSnapshotter<H extends SnapshotHeader, B extends SnapshotBody, F extends SnapshotFooter> {

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
            throw new IllegalArgumentException("Entries cannot be null or empty");
        }

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        final H header = SnapshotRegistry.createHeader(headerClass, entries);
        final byte[] headerBytes = header.serialize();
        buffer = expandBuffer(buffer.flip(), buffer.limit() + Integer.BYTES + headerBytes.length);
        buffer.putInt(headerBytes.length);
        buffer.put(headerBytes);

        final B body = SnapshotRegistry.createBody(bodyClass, entries);
        final byte[] bodyBytes = body.serialize();
        buffer = expandBuffer(buffer.flip(), buffer.limit() + Integer.BYTES + bodyBytes.length);
        buffer.putInt(bodyBytes.length);
        buffer.put(bodyBytes);

        final F footer = SnapshotRegistry.createFooter(footerClass, entries);
        final byte[] footerBytes = footer.serialize();
        buffer = expandBuffer(buffer.flip(), buffer.limit() + Integer.BYTES + footerBytes.length);
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
            byte[] headerLenBytes = is.readNBytes(4);
            final int headerLen = ByteBuffer.wrap(headerLenBytes).getInt();
            is.readNBytes(headerLen);

            byte[] bodyLenBytes = is.readNBytes(4);
            final int bodyLen = ByteBuffer.wrap(bodyLenBytes).getInt();
            return SnapshotRegistry.deserializeBody(bodyClass, ByteBuffer.wrap(is.readNBytes(bodyLen)));
        }
    }

    public F getFooter(final Path snapshotPath) throws IOException {
        try (final InputStream is = new FileInputStream(snapshotPath.toString())) {
            ByteBuffer buffer = ByteBuffer.allocate(4);

            byte[] headerLenBytes = is.readNBytes(4);
            buffer.put(headerLenBytes);
            buffer.flip();
            int headerLen = buffer.getInt();
            is.readNBytes(headerLen);

            buffer.clear();
            byte[] bodyLenBytes = is.readNBytes(4);
            buffer.put(bodyLenBytes);
            buffer.flip();
            int bodyLen = buffer.getInt();
            is.readNBytes(bodyLen);

            buffer.clear();
            byte[] footerLenBytes = is.readNBytes(4);
            buffer.put(footerLenBytes);
            buffer.flip();
            int footerLen = buffer.getInt();

            return SnapshotRegistry.deserializeFooter(footerClass, ByteBuffer.wrap(is.readNBytes(footerLen)));
        }
    }

    public Snapshot<H, B, F> getSnapshot(final Path snapshotPath) throws IOException {
        final H header = getHeader(snapshotPath);
        final B body = getBody(snapshotPath);
        final F footer = getFooter(snapshotPath);
        return new Snapshot<>(header, body, footer);
    }

    /**
     * Finds the path to a snapshot file which will contain a log entry with the provided log id
     *
     * @param logId Log ID to search for
     * @return File path to a snapshot file which may contain a log entry with the provided log id
     */
    public Path findSnapshot(final long logId) {
        final File[] files = dir.toFile().listFiles();
        if (files == null) {
            return null;
        }

        for (File file : files) {
            final Path path = file.toPath();
            final long lastLogId =
                    Long.parseLong(path.getFileName().toString().replace(".snapshot", ""));
            if (lastLogId >= logId) {
                return path;
            }
        }

        return null;
    }

    public Path getFpath(final List<LogEntry> entries) {
        final LogEntry logEntry = entries.getLast();
        return dir.resolve(logEntry.id + ".snapshot");
    }
}
