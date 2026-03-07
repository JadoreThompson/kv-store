package com.zenz.kvstore;

import com.zenz.kvstore.commands.Command;

public interface BaseLog {
    long id();

    Command command();
}
