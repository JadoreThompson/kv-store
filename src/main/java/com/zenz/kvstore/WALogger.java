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
//
//    public void log(Log logObject) throws IOException {
//        ByteBuffer buffer = ByteBuffer.wrap(
//                (logObject.id + " " + logObject.operation.getValue() + " " + logObject.message + "\n").getBytes(StandardCharsets.UTF_8)
//        );
//
//        while (buffer.hasRemaining()) {
//            channel.write(buffer);
//        }
//
//        channel.write(buffer);
//        channel.force(true);
//    }

    public void logPut(int id, OperationType opType, String key, byte[] value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put((id + " " + opType.getValue() + " " + key + " ").getBytes(StandardCharsets.UTF_8));
        buffer.put(value);
        buffer.put("\n".getBytes(StandardCharsets.UTF_8));
        buffer.flip();

//        ByteBuffer buffer = ByteBuffer.wrap((id + " " + opType.getValue() + " " + key).getBytes(StandardCharsets.UTF_8));

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        channel.write(buffer);
        channel.force(true);
    }

    public void logGet(int id, OperationType opType, String key) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        buffer.put((id + " " + opType.getValue() + " " + key + "\n").getBytes(StandardCharsets.UTF_8));
        buffer.flip();
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
