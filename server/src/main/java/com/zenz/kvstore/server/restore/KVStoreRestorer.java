package com.zenz.kvstore.server.restore;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.*;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.SnapshotBody;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class KVStoreRestorer {

    public KVStore restore(final KVStore kvStore) throws IOException {
        applySnapshots(kvStore);
        applyLogEntries(kvStore);
        return kvStore;
    }

    protected void applySnapshots(final KVStore kvStore) throws IOException {
        final KVStoreSnapshotter<?, ?, ?> snapshotter = kvStore.getLogHandler().getSnapshotter();
        final Path snapshotDir = snapshotter.getDir();
        final File[] snapshotFiles = snapshotDir.toFile().listFiles();

        final BaseLogHandler<?, ?> prevLogHandler = kvStore.getLogHandler();
        kvStore.setLogHandler(new NoOpLogHandler<>());

        if (snapshotFiles != null) {
            for (File snapshotFile : snapshotFiles) {
                final SnapshotBody body = snapshotter.getBody(snapshotFile.toPath());
                for (LogEntry logEntry : body.getEntries()) {
                    applyLogEntry(logEntry, kvStore);
                }
            }
        }

        kvStore.setLogHandler(prevLogHandler);
    }

    protected List<? extends LogEntry> applyLogEntries(final KVStore kvStore) throws IOException {
        final BaseLogHandler<?, ?> logHandler = kvStore.getLogHandler();
        final List<? extends LogEntry> logEntries = logHandler.loadLogs(kvStore.getLogHandler().getLogger().getPath());
        final CommandLogger prevLogger = logHandler.getLogger();
        logHandler.setLogger(new NoOpLogger());

        for (LogEntry logEntry : logEntries) {
            applyLogEntry(logEntry, kvStore);
        }

        logHandler.setLogger(prevLogger);
        return logEntries;
    }

    protected void applyLogEntry(final LogEntry logEntry, final KVStore kvStore) throws IOException {
        kvStore.getLogHandler().setLogId(logEntry.id - 1);

        switch (logEntry.command.type()) {
            case PUT -> {
                final PutCommand comm = (PutCommand) logEntry.command;
                kvStore.put(comm.key(), comm.value());
            }
            case DELETE -> {
                final DeleteCommand comm = (DeleteCommand) logEntry.command;
                kvStore.delete(comm.key());
            }
        }
    }

    protected static class NoOpLogger implements CommandLogger {

        @Override
        public void log(LogEntry logEntry) throws IOException {

        }

        @Override
        public Path getPath() {
            return null;
        }

        @Override
        public LoggerFactory getLoggerFactory() {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public <L extends LogEntry> List<L> loadLogs(Path path, Deserializer<L> deserializer) throws IOException {
            return List.of();
        }
    }
}
