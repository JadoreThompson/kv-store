package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.command.GetCommand;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Getter
public class RaftLogHandler implements BaseLogHandler<RaftLogEntry> {

    private static final int LOGS_PER_SNAPSHOT = 100_000;

    @Setter
    private Logger logger;
    @Setter
    private long logId;
    @Setter
    private long term;
    @Setter
    private Snapshotter<RaftLogEntry> snapshotter;

    private List<RaftLogEntry> entries = new ArrayList<>();
    private RaftLogEntry seedEntry = new RaftLogEntry(0L, 0L, new GetCommand(""));


    public RaftLogHandler(final Logger logger, final Snapshotter<RaftLogEntry> snapshotter) {
        this.logger = logger;
        this.snapshotter = snapshotter;
    }

    @Override
    public RaftLogEntry log(final Command command) throws IOException {
        if (entries.size() == LOGS_PER_SNAPSHOT) {
            seedEntry = entries.getLast();
            snapshotter.snapshot(entries);
            entries = new ArrayList<>();
        }

        final RaftLogEntry logEntry = new RaftLogEntry(++logId, term, command);
        logger.log(logEntry);
        entries.add(logEntry);
        return logEntry;
    }

    /**
     * @return Returns the first entry in the buffer. Note that upon each snapshot the buffer is wiped
     */
    public RaftLogEntry getFirstEntry() {
        return entries.isEmpty() ? null : entries.getFirst();
    }

    /**
     * @return Returns the last entry in the buffer. Note that upon each snapshot the buffer is wiped
     */
    public RaftLogEntry getLastEntry() {
        return entries.isEmpty() ? null : entries.getLast();
    }
}
