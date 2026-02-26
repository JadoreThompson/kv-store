package com.zenz.kvstore.log_handlers;

import com.zenz.kvstore.WALogger;
import com.zenz.kvstore.commands.Command;

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
