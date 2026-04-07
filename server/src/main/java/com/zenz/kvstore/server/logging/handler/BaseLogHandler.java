package com.zenz.kvstore.server.logging.handler;

import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.server.logging.WALogger;

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
