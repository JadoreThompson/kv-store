package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;

@Getter
@RequiredArgsConstructor
public class LogEntry {

    public final long id;

    public final Command command;

    public byte[] serialize() {
        final byte[] commandBytes = command.serialize();
        final ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + commandBytes.length);

        buffer.putLong(id);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }

    public static LogEntry deserialize(final ByteBuffer buffer) {
        final long id = buffer.getLong();
        final int commandLength = buffer.getInt();
        final byte[] commandBytes = new byte[commandLength];
        buffer.get(commandBytes);
        final Command command = Command.deserialize(ByteBuffer.wrap(commandBytes));

        return new LogEntry(id, command);
    }
}