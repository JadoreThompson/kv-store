package com.zenz.kvstore.server;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ClientSession {
    public static final int BUFFER_SIZE = 8192;
    private final SocketChannel channel;
    private ByteBuffer readBuffer;

    public ClientSession(SocketChannel channel) {
        this.channel = channel;
        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    }

    public ClientSession(SocketChannel channel, int bufferSize) {
        this.channel = channel;
        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public void setReadBuffer(ByteBuffer readBuffer) {
        this.readBuffer = readBuffer;
    }
}
