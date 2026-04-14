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

    @Override
    public String toString() {
        return "RaftLogEntry{" +
                "term=" + term +
                ", id=" + id +
                ", command=" + command +
                '}';
    }
}