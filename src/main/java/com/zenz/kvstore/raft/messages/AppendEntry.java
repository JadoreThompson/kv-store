package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.commands.Command;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record AppendEntry(
        MessageType type,
        long id,
        long term,
        Command command
) implements BaseMessage {

    public AppendEntry(
            long id,
            long term,
            Command command
    ) {
        this(MessageType.APPEND_ENTRY, id, term, command);
    }

    @Override
    public byte[] serialize() {
        byte[] commandBytes = command != null ? command.serialize() : new byte[0];
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + commandBytes.length);

        buffer.putInt(type.getValue());
        buffer.putLong(id);
        buffer.putLong(term);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }

    public static AppendEntry deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (!messageType.equals(MessageType.APPEND_ENTRY)) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            long id = buffer.getLong();
            long term = buffer.getLong();
            int commandLength = buffer.getInt();
            byte[] commandBytes = new byte[commandLength];
            buffer.get(commandBytes);

            Command command = Command.deserialize(commandBytes);

            return new AppendEntry(messageType, id, term, command);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}