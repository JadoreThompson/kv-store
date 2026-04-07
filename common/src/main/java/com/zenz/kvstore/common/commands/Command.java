package com.zenz.kvstore.common.commands;

import com.zenz.kvstore.common.enums.CommandType;

import java.nio.ByteBuffer;

public interface Command {

    CommandType type();

    byte[] serialize();

    static Command deserialize(ByteBuffer buffer) {
        int typeValue = buffer.getInt();
        CommandType type = CommandType.fromValue(typeValue);

        buffer.rewind();
        if (type.equals(CommandType.PUT)) return PutCommand.deserialize(buffer);
        if (type.equals(CommandType.GET)) return GetCommand.deserialize(buffer);
        if (type.equals(CommandType.DELETE)) return DeleteCommand.deserialize(buffer);

        throw new IllegalArgumentException("Unknown command type value: " + typeValue);
    }

//    static ArrayList<Command> deserializeList(ByteBuffer buffer) {
//        ArrayList<Command> commands = new ArrayList<>();
//
//        while (buffer.hasRemaining()) {
//            int typeValue = buffer.getInt();
//            CommandType type = CommandType.fromValue(typeValue);
//            int keyLength = buffer.getInt();
//            byte[] keyBytes = new byte[keyLength];
//            buffer.get(keyBytes);
//
//            Command command;
//            if (type.equals(CommandType.PUT)) {
//                ByteBuffer commandBuffer = ByteBuffer.allocate(1024);
//
//                int valueLength = buffer.getInt();
//                byte[] valueBytes = new byte[valueLength];
//                buffer.get(valueBytes);
//
//                commandBuffer.putInt(typeValue);
//                commandBuffer.putInt(keyLength);
//                commandBuffer.put(keyBytes);
//                commandBuffer.putInt(valueLength);
//                commandBuffer.put(valueBytes);
//
//                command = PutCommand.deserialize(commandBuffer.array());
//            } else if (type.equals(CommandType.GET)) {
//                ByteBuffer commandBuffer = ByteBuffer.allocate(1024);
//
//                commandBuffer.putInt(typeValue);
//                commandBuffer.putInt(keyLength);
//                commandBuffer.put(keyBytes);
//
//                command = GetCommand.deserialize(commandBuffer.array());
//            } else {
//                throw new IllegalArgumentException("Unknown command errorType: " + type);
//            }
//            commands.add(command);
//
//            if (buffer.hasRemaining()) buffer.get(); // Skipping new line char
//        }
//
//        return commands;
//    }
}