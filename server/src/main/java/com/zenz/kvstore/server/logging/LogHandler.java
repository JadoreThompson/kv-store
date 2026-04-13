package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotBody;
import com.zenz.kvstore.server.snapshot.SingleSnapshotFooter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotHeader;
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
public class LogHandler implements BaseLogHandler<
        LogEntry, KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter>> {

    private static final int LOGS_PER_SNAPSHOT = 100_000;

    @Setter
    private int logsPerSnapshot = LOGS_PER_SNAPSHOT;

    @Setter
    private CommandLogger logger;

    @Setter
    private long logId;

    @Setter
    private KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter;

    private List<LogEntry> entries = new ArrayList<>();

    public LogHandler(
            final CommandLogger logger,
            final KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter
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
        final Deserializer<LogEntry> deserializer = new LogEntryDeserializer();
        final List<LogEntry> entries = new ArrayList<>();

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
