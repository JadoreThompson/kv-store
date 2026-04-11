package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;

public interface BaseLogEntry {

    long id();

    Command command();
}
