package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Getter
public class RaftLogHandler implements BaseLogHandler<RaftLogEntry> {

    private static final int LOGS_PER_SNAPSHOT = 100_000;

    @Getter
    @Setter
    private Logger logger;
    @Getter
    @Setter
    private long logId;
    @Getter
    @Setter
    private long term;
    private Snapshotter<RaftLogEntry> snapshotter;
    @Setter
    private boolean enabled;
    private List<RaftLogEntry> entries = new ArrayList<>();

    public RaftLogHandler(final Logger logger, final Snapshotter<RaftLogEntry> snapshotter) {
        this.logger = logger;
        this.snapshotter = snapshotter;
    }

    @Override
    public void log(final Command command) throws IOException {
        if (entries.size() == LOGS_PER_SNAPSHOT) {
            snapshotter.snapshot(entries);
            entries = new ArrayList<>();
        }

        final RaftLogEntry logEntry = new RaftLogEntry(++logId, term, command);
        entries.add(logEntry);
        logger.log(ByteBuffer.wrap(logEntry.serialize()));
    }

    @Override
    public List<RaftLogEntry> getLogs() {
        return entries;
    }

    @Override
    public RaftLogEntry getFirstEntry() {
        return entries.isEmpty() ? null : entries.getFirst();
    }

    @Override
    public RaftLogEntry getLastEntry() {
        return entries.isEmpty() ? null : entries.getLast();
    }

    @Override
    public void setLogger(final Logger logger) {
        this.logger = logger;
    }

    @Override
    public Snapshotter<RaftLogEntry> getSnapshotter() {
        return snapshotter;
    }

    @Override
    public void setSnapshotter(final Snapshotter<RaftLogEntry> snapshotter) {
        this.snapshotter = snapshotter;
    }

}
