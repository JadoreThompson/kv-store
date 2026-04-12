package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.command.GetCommand;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotBody;
import com.zenz.kvstore.server.snapshot.RaftSnapshotFooter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotHeader;
import lombok.Getter;
import lombok.Setter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Getter
public class RaftLogHandler implements BaseLogHandler<
        RaftLogEntry, KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter>> {

    private static final int LOGS_PER_SNAPSHOT = 100_000;

    @Setter
    private Logger logger;

    @Setter
    private long logId;

    @Setter
    private long term;

    @Setter
    private KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter;

    private List<RaftLogEntry> entries = new ArrayList<>();

    private RaftLogEntry seedEntry = new RaftLogEntry(0L, 0L, new GetCommand(""));

    public RaftLogHandler(
            final Logger logger,
            final KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter
    ) {
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

    @Override
    public List<RaftLogEntry> loadLogs(final Path path) throws IOException {
        final Deserializer<RaftLogEntry> deserializer = new RaftLogEntryDeserializer();
        final List<RaftLogEntry> entries = new ArrayList<>();

        try (final InputStream is = new FileInputStream(path.toString())) {
            ByteBuffer lenBuffer = ByteBuffer.allocate(4);

            while (true) {
                byte[] bytes = is.readNBytes(4);
                if (bytes.length != 4) {
                    break;
                }

                lenBuffer.putInt(bytes.length);
                final int len = lenBuffer.getInt();
                lenBuffer.clear();

                ByteBuffer buffer = ByteBuffer.allocate(len);
                lenBuffer.put(is.readNBytes(len));
                entries.add(deserializer.deserialize(buffer));
            }
        }

        return entries;
    }
}
