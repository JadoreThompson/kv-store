package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Getter
public class LogHandler implements BaseLogHandler<LogEntry> {

    private static final int LOGS_PER_SNAPSHOT = 100_000;

    @Setter
    private Logger logger;
    @Setter
    private long logId;
    @Setter
    private Snapshotter<LogEntry> snapshotter;
    private List<LogEntry> entries = new ArrayList<>();

    public LogHandler(final Logger logger, final Snapshotter<LogEntry> snapshotter) {
        this.logger = logger;
        this.snapshotter = snapshotter;
    }

    @Override
    public void log(final Command command) throws IOException {
        if (entries.size() == LOGS_PER_SNAPSHOT) {
            snapshotter.snapshot(entries);
            entries = new ArrayList<>();
        }

        final LogEntry logEntry = new LogEntry(++logId, command);
        logger.log(logEntry);
        entries.add(logEntry);
    }
}
