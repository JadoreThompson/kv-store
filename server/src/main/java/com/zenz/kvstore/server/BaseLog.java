package com.zenz.kvstore.server;

import com.zenz.kvstore.server.commands.Command;

public interface BaseLog {
    long id();

    Command command();
}
