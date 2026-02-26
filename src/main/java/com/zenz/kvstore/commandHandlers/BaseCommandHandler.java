package com.zenz.kvstore.commandHandlers;

import com.zenz.kvstore.commands.Command;

import java.nio.ByteBuffer;

public interface BaseCommandHandler {
    ByteBuffer handleCommand(Command command);
}
