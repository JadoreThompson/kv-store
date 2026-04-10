package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
public class RaftLogEntry extends LogEntry {

    public final long term;

    public RaftLogEntry(final long id, final long term, final Command command) {
        super(id, command);
        this.term = term;
    }


    @Override
    public byte[] serialize() {
        final byte[] commandBytes = command.serialize();
        final ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 4 + commandBytes.length);

        buffer.putLong(id);
        buffer.putLong(term);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }

    public static RaftLogEntry deserialize(final ByteBuffer buffer) {
        final long id = buffer.getLong();
        final long term = buffer.getLong();
        final int commandLength = buffer.getInt();
        final byte[] commandBytes = new byte[commandLength];
        buffer.get(commandBytes);
        final Command command = Command.deserialize(ByteBuffer.wrap(commandBytes));

        return new RaftLogEntry(id, term, command);
    }
}