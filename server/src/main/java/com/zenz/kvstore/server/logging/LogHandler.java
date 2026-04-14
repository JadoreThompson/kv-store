package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Getter
public class LogHandler implements BaseLogHandler<
        LogEntry, KVStoreSnapshotter<?, ?, ?>> {

    public static final int LOGS_PER_SNAPSHOT = 100_000;

    @Setter
    private int logsPerSnapshot = LOGS_PER_SNAPSHOT;

    @Setter
    private CommandLogger logger;

    @Setter
    private long logId;

    @Setter
    private KVStoreSnapshotter<?, ?, ?> snapshotter;

    private List<LogEntry> entries = new ArrayList<>();

    public LogHandler(
            final CommandLogger logger,
            final KVStoreSnapshotter<?, ?, ?> snapshotter
    ) {
        this.logger = logger;
        this.snapshotter = snapshotter;
    }

    @Override
    public LogEntry log(final Command command) throws IOException {
        final LogEntry logEntry = new LogEntry(++logId, command);
        logger.log(logEntry);
        entries.add(logEntry);

        if (entries.size() == logsPerSnapshot) {
            snapshotter.snapshot(entries);
            entries = new ArrayList<>();
        }

        return logEntry;
    }

    @Override
    public List<LogEntry> loadLogs(Path path) throws IOException {
        return logger.loadLogs(path, new LogEntryDeserializer());
    }
}
