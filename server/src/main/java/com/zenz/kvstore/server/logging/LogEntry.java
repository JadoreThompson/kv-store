package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.util.ByteArraySerializable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;

@Getter
@RequiredArgsConstructor
public class LogEntry implements ByteArraySerializable {

    public final long id;

    public final Command command;

    @Override
    public byte[] serialize() {
        final byte[] commandBytes = command != null ? command.serialize() : new byte[0];
        final ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + commandBytes.length);

        buffer.putLong(id);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }
}