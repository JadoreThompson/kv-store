package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;

import java.nio.ByteBuffer;

public class RaftLogEntryDeserializer implements Deserializer<RaftLogEntry> {

    static {
        LogEntryRegister.register(RaftLogEntry.class, new RaftLogEntryDeserializer());
    }

    @Override
    public RaftLogEntry deserialize(final ByteBuffer buffer) {
        final long id = buffer.getLong();
        final long term = buffer.getLong();
        final int commandLength = buffer.getInt();
        if (commandLength == 0) {
            return new RaftLogEntry(id, term, null);
        }
        final byte[] commandBytes = new byte[commandLength];
        buffer.get(commandBytes);
        final Command command = Command.deserialize(ByteBuffer.wrap(commandBytes));

        return new RaftLogEntry(id, term, command);
    }
}
