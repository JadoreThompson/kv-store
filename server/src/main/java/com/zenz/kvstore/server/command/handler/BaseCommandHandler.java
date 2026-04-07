package com.zenz.kvstore.server.command.handler;

import com.zenz.kvstore.common.command.Command;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface BaseCommandHandler {

    default ByteBuffer handleCommand(Command command) {
        throw new UnsupportedOperationException();
    }

    default ByteBuffer handleCommand(SocketChannel channel, Command command) {
        throw new UnsupportedOperationException();
    }
}
