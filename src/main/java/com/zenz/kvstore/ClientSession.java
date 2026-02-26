package com.zenz.kvstore;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ClientSession {
    public static final int BUFFER_SIZE = 8192;
    private final SocketChannel channel;
    private final ByteBuffer readBuffer;
    private final int bufferSize;

    public ClientSession(SocketChannel channel) {
        this.channel = channel;
        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.bufferSize = BUFFER_SIZE;
    }

    public ClientSession(SocketChannel channel, int bufferSize) {
        this.channel = channel;
        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.bufferSize = bufferSize;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
