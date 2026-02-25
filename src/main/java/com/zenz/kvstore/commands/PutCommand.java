package com.zenz.kvstore.commands;

import com.zenz.kvstore.CommandType;

import java.nio.charset.StandardCharsets;

public record PutCommand(int id, String key, byte[] value) implements Command {

    @Override
    public CommandType type() {
        return CommandType.PUT;
    }

    public static PutCommand fromLine(int id, String[] components) {
        String key = components[2];
        String valueStr = components[3];
        return new PutCommand(id, key, valueStr.getBytes(StandardCharsets.UTF_8));
    }
}