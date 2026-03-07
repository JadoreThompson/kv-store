package com.zenz.kvstore.server.logHandlers;

import com.zenz.kvstore.server.WALogger;
import com.zenz.kvstore.server.commands.Command;

import java.io.IOException;

public interface BaseLogHandler {
    void log(Command command) throws IOException;

    WALogger getLogger();

    void setLogger(WALogger logger);

    long getLogId();

    void setLogId(long logId);

    boolean isDisabled();

    void setDisabled(boolean disabled);
}
