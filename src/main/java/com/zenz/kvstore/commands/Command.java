package com.zenz.kvstore.commands;

import com.zenz.kvstore.CommandType;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public interface Command {

    CommandType type();

    byte[] serialize();

    static Command deserialize(byte[] bytes) {
        if (bytes.length == 0) return null;

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int typeValue = buffer.getInt();
        CommandType type = CommandType.fromValue(typeValue);

        if (type.equals(CommandType.PUT)) return PutCommand.deserialize(bytes);
        if (type.equals(CommandType.GET)) return GetCommand.deserialize(bytes);

        throw new IllegalArgumentException("Unknown command type: " + typeValue);
    }

    static ArrayList<Command> deserializeList(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        ArrayList<Command> commands = new ArrayList<>();

        while (buffer.hasRemaining()) {
            int typeValue = buffer.getInt();
            CommandType type = CommandType.fromValue(typeValue);
            int keyLength = buffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);

            Command command;
            if (type.equals(CommandType.PUT)) {
                ByteBuffer commandBuffer = ByteBuffer.allocate(1024);

                int valueLength = buffer.getInt();
                byte[] valueBytes = new byte[valueLength];
                buffer.get(valueBytes);

                commandBuffer.putInt(typeValue);
                commandBuffer.putInt(keyLength);
                commandBuffer.put(keyBytes);
                commandBuffer.putInt(valueLength);
                commandBuffer.put(valueBytes);

                command = PutCommand.deserialize(commandBuffer.array());
            } else if (type.equals(CommandType.GET)) {
                ByteBuffer commandBuffer = ByteBuffer.allocate(1024);

                commandBuffer.putInt(typeValue);
                commandBuffer.putInt(keyLength);
                commandBuffer.put(keyBytes);

                command = GetCommand.deserialize(commandBuffer.array());
            } else {
                throw new IllegalArgumentException("Unknown command type: " + type);
            }
            commands.add(command);

            if (buffer.hasRemaining()) buffer.get(); // Skipping new line char
        }

        return commands;
    }
}