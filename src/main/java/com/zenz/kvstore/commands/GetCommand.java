package com.zenz.kvstore.commands;

import com.zenz.kvstore.CommandType;

public record GetCommand(int id, String key) implements Command {

    @Override
    public CommandType type() {
        return CommandType.GET;
    }

    public static GetCommand deserialize(int id, String[] components) {
        String key = components[2];
        return new GetCommand(id, key);
    }
}