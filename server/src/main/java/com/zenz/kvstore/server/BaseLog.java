package com.zenz.kvstore.server;

import com.zenz.kvstore.common.commands.Command;

public interface BaseLog {
    long id();

    Command command();
}
