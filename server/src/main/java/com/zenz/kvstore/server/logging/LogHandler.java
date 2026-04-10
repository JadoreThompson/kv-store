package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Getter
public class LogHandler implements BaseLogHandler<LogEntry> {

    private static final int LOGS_PER_SNAPSHOT = 100_000;

    private Logger logger;
    private long logId;
    @Setter
    private Snapshotter snapshotter;
    @Setter
    private boolean enabled;
    private List<LogEntry> entries = new ArrayList<>();

    public LogHandler(final Logger logger, final Snapshotter snapshotter) {
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
        entries.add(logEntry);

        if (enabled) {
            logger.log(ByteBuffer.wrap(logEntry.serialize()));
        }
    }

    @Override
    public List<LogEntry> getLogs() {
        return entries;
    }

    @Override
    public LogEntry getFirstEntry() {
        return entries.isEmpty() ? null : entries.getFirst();
    }

    @Override
    public LogEntry getLastEntry() {
        return entries.isEmpty() ? null : entries.getLast();
    }

    @Override
    public void setLogger(final Logger logger) {
        this.logger = logger;
    }

    @Override
    public Snapshotter getSnapshotter() {
        return snapshotter;
    }
}
