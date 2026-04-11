package com.zenz.kvstore.server.raft;

import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.net.InetSocketAddress;

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

    public Client(final InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public void open() {
    }


    public void close() {
    }
}
