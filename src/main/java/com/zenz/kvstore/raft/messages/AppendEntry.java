package com.zenz.kvstore.raft.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.commands.Command;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public record AppendEntry(
        MessageType type,
        long id,
        long term,
        List<Command> commands
) implements BaseMessage {

    public AppendEntry(
            long id,
            long term,
            List<Command> commands
    ) {
        this(MessageType.APPEND_ENTRY, id, term, commands);
    }

    @Override
    public byte[] serialize() {
        // Calculate total size needed for commands
        int commandsSize = 0;
        List<byte[]> commandBytesList = new ArrayList<>();

        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd == null) continue;

                byte[] cmdBytes = cmd.serialize();
                commandBytesList.add(cmdBytes);
                commandsSize += 4 + cmdBytes.length; // 4 bytes for length + command bytes
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + commandsSize);

        buffer.putInt(type.getValue());
        buffer.putLong(id);
        buffer.putLong(term);
        buffer.putInt(commandsSize);

        for (byte[] cmdBytes : commandBytesList) {
            buffer.putInt(cmdBytes.length);
            buffer.put(cmdBytes);
        }

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

            int allCommandBytesLength = buffer.getInt();
            byte[] allCommandBytes = new byte[allCommandBytesLength];
            buffer.get(allCommandBytes);
            ByteBuffer allCommandBuffer = ByteBuffer.wrap(allCommandBytes);

            List<Command> commands = new ArrayList<>();
            while (allCommandBuffer.hasRemaining()) {
                int commandLength = allCommandBuffer.getInt();
                byte[] commandBytes = new byte[commandLength];
                allCommandBuffer.get(commandBytes);
                Command command = Command.deserialize(commandBytes);
                commands.add(command);
            }

            return new AppendEntry(id, term, commands);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "AppendEntry{" +
                "type=" + type +
                ", id=" + id +
                ", term=" + term +
                ", commands=" + commands +
                '}';
    }
}
