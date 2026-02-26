package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.commands.Command;

import java.nio.ByteBuffer;

/**
 * Sent by the controller, this object contains the most recent log
 * seen by the controller.
 *
 * @param type
 * @param logId
 * @param term
 * @param command
 */
public record LogBroadcast(MessageType type, long logId, long term, Command command) implements Message {
    public LogBroadcast(long logId, long term, Command command) {
        this(MessageType.LOG_BROADCAST, logId, term, command);
    }

    @Override
    public byte[] serialize() {
        byte[] commandBytes = command.serialize();
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + commandBytes.length);

        buffer.putInt(type.getValue());
        buffer.putLong(logId);
        buffer.putLong(term);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }

    public static LogBroadcast deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int typeValue = buffer.getInt();
        MessageType type = MessageType.fromValue(typeValue);
        if (!type.equals(MessageType.LOG_RESPONSE)) {
            throw new IllegalArgumentException("Invalid message type");
        }

        long logId = buffer.getLong();
        long term = buffer.getLong();
        int commandByteLength = buffer.getInt();
        byte[] commandBytes = new byte[commandByteLength];
        buffer.get(commandBytes);
        Command command = Command.deserialize(commandBytes);

        return new LogBroadcast(logId, term, command);
    }
}

