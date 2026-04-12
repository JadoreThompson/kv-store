package com.zenz.kvstore.server.logging;

import lombok.Getter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WALogger implements Logger, Closeable {

    public static final Path DEFAULT_PATH = Path.of("app.log");

    @Getter
    private Path path = DEFAULT_PATH;

    private FileChannel channel;

    public WALogger() throws IOException {
        openChannel(path);
    }

    public WALogger(Path path) throws IOException {
        openChannel(path);
        this.path = path;
    }

    private void openChannel(final Path fpath) throws IOException {
        File file = fpath.toFile();
        if (!file.exists() && !file.createNewFile()) {
            throw new IOException("Failed to create file " + file.getPath());
        }

        channel = FileChannel.open(fpath, StandardOpenOption.APPEND);
    }

    /**
     * Persists the log entry to a file
     *
     * @param logEntry Log entry to persist
     * @throws IOException During write operation
     */
    @Override
    public void log(final LogEntry logEntry) throws IOException {
        final byte[] bytes = logEntry.serialize();
        final ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        channel.write(buffer);
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public WALoggerFactory getLoggerFactory() {
        return new WALoggerFactory();
    }
}