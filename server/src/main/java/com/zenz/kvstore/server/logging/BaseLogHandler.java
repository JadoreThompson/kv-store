package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;

import java.io.IOException;

public interface BaseLogHandler<L extends LogEntry> {

    L log(Command command) throws IOException;

    Logger getLogger();

    void setLogger(Logger logger);

    Snapshotter<L> getSnapshotter();

    void setSnapshotter(Snapshotter<L> snapshotter);

    long getLogId();

    void setLogId(long logId);
}
