package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;

import java.io.IOException;
import java.util.List;

public interface BaseLogHandler<L extends LogEntry> {

    void log(Command command) throws IOException;

    List<L> getLogs();

    L getFirstEntry() throws IOException;

    L getLastEntry() throws IOException;

    boolean isEnabled();

    void setEnabled(boolean enabled);

    Logger getLogger();

    void setLogger(Logger logger);

    Snapshotter getSnapshotter();

    void setSnapshotter(Snapshotter snapshotter);
}
