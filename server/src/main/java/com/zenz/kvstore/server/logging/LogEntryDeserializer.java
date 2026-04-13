package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;

import java.nio.ByteBuffer;

public class LogEntryDeserializer implements Deserializer<LogEntry> {

    static {
        LogEntryRegister.register(LogEntry.class, new LogEntryDeserializer());
    }

    @Override
    public LogEntry deserialize(final ByteBuffer buffer) {
        final long id = buffer.getLong();
        final int commandLength = buffer.getInt();
        if (commandLength == 0) {
            return new LogEntry(id, null);
        }

        final byte[] commandBytes = new byte[commandLength];
        buffer.get(commandBytes);
        final Command command = Command.deserialize(ByteBuffer.wrap(commandBytes));

        return new LogEntry(id, command);
    }
}
