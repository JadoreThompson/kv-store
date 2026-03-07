package com.zenz.kvstore.server.commandHandlers;

import com.zenz.kvstore.server.commands.Command;

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
