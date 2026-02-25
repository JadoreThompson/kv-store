package com.zenz.kvstore.commands;

import com.zenz.kvstore.CommandType;

public interface Command {
    int id();

    CommandType type();

    static Command fromLine(String line) {
        String[] components = line.strip().split(" ");
        int id = Integer.parseInt(components[0]);
        CommandType type = CommandType.valueOf(components[1]);

        return switch (type) {
            case PUT -> PutCommand.fromLine(id, components);
            case GET -> GetCommand.fromLine(id, components);
            default -> throw new UnsupportedOperationException("Unsupported command " + type.getValue());
        };
    }
}