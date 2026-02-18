package com.zenz.kvstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WALogger {
    private FileChannel channel;

    public WALogger(String fPath) throws IOException {
        File file = new File(fPath);
        if (!file.exists()) {
            file.createNewFile();
        }
        channel = FileChannel.open(Path.of(fPath), StandardOpenOption.APPEND);
    }

    public WALogger(String fPath, int batchSize) throws IOException {
        this(fPath);
    }

    public void log(String message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(
                (message + "\n").getBytes(StandardCharsets.UTF_8)
        );

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        channel.write(buffer);
        channel.force(true);
    }

    public void close() throws IOException {
        channel.close();
    }
}
