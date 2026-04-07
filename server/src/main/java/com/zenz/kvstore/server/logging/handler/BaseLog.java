package com.zenz.kvstore.server.logging.handler;

import com.zenz.kvstore.common.commands.Command;

public interface BaseLog {
    long id();

    Command command();
}
