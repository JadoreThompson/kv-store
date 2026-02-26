package com.zenz.kvstore.command_handlers;

import com.zenz.kvstore.commands.Command;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface BaseCommandHandler {
    ByteBuffer handleCommand(Command command);
}
