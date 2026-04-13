package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface BaseLogHandler<L extends LogEntry, S extends KVStoreSnapshotter<?, ?, ?>> {

    L log(Command command) throws IOException;

    CommandLogger getLogger();

    void setLogger(CommandLogger logger);

    S getSnapshotter();

    void setSnapshotter(S snapshotter);

    long getLogId();

    void setLogId(long logId);

    List<L> loadLogs(Path fpath) throws IOException;

    void setLogsPerSnapshot(int logsPerSnapshot);

    int getLogsPerSnapshot();
}
