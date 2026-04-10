package com.zenz.kvstore.server.logging;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WALogger implements Logger, Closeable {

    private final FileChannel channel;
    private final Path path;

    public WALogger(Path fpath) throws IOException {
        File file = fpath.toFile();
        if (!file.exists() && !file.createNewFile()) {
            throw new IOException("Failed to create file " + file.getPath());
        }

        this.path = fpath;
        channel = FileChannel.open(path, StandardOpenOption.APPEND);
    }

    public void log(ByteBuffer buffer) throws IOException {
        channel.write(buffer);
        channel.write(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));
        channel.force(true);
    }

    public void close() throws IOException {
        channel.close();
    }

    public Path getPath() {
        return path;
    }
}
