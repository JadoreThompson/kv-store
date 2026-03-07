package com.zenz.kvstore.server;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface SocketHandler {
    void init() throws Exception;

    void handleAccept(SelectionKey key) throws IOException;

    void handleRead(SelectionKey key) throws IOException;

    void handleWrite(SelectionKey key) throws IOException;

    default void handleWakeUp() {
    }
}
