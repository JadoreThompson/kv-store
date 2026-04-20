package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.command.GetCommand;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotBody;
import com.zenz.kvstore.server.snapshot.RaftSnapshotFooter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotHeader;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Getter
public class RaftLogHandler implements BaseLogHandler<
        RaftLogEntry, KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter>> {

    private static final int LOGS_PER_SNAPSHOT = 100_000;

    @Setter
    private int logsPerSnapshot = LOGS_PER_SNAPSHOT;

    @Setter
    private CommandLogger logger;

    @Setter
    private long logId;

    @Setter
    private long term;

    @Setter
    private KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter;

    @Setter
    private List<RaftLogEntry> entries = new ArrayList<>();

    private RaftLogEntry seedEntry = new RaftLogEntry(0L, 0L, new GetCommand("\0"));

    /**
     * Default constructor for Mockito mocking
     */
    public RaftLogHandler() throws IOException {
        logger = new WALogger();
        snapshotter = new KVStoreSnapshotter<>(
                RaftSnapshotHeader.class,
                RaftSnapshotBody.class,
                RaftSnapshotFooter.class);
    }

    public RaftLogHandler(
            final CommandLogger logger,
            final KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter
    ) {
        this.logger = logger;
        this.snapshotter = snapshotter;
    }

    @Override
    public RaftLogEntry log(final Command command) throws IOException {
        if (entries.size() == logsPerSnapshot) {
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

    @Override
    public List<RaftLogEntry> loadLogs(final Path path) throws IOException {
        return logger.loadLogs(path, new RaftLogEntryDeserializer());
    }
}
