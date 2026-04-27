package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class NoOpLogHandler<L extends LogEntry> implements BaseLogHandler<L, KVStoreSnapshotter<?, ?, ?>> {

    @Override
    public L log(Command command) throws IOException {
        return null;
    }

    @Override
    public CommandLogger getLogger() {
        return null;
    }

    @Override
    public void setLogger(CommandLogger logger) {

    }

    @Override
    public KVStoreSnapshotter<?, ?, ?> getSnapshotter() {
        return null;
    }

    @Override
    public void setSnapshotter(KVStoreSnapshotter<?, ?, ?> snapshotter) {

    }

    @Override
    public long getLogId() {
        return 0;
    }

    @Override
    public void setLogId(long logId) {

    }

    @Override
    public List<L> loadLogs(Path fpath) throws IOException {
        return List.of();
    }

    @Override
    public void setLogsPerSnapshot(int logsPerSnapshot) {

    }

    @Override
    public int getLogsPerSnapshot() {
        return 0;
    }

}
