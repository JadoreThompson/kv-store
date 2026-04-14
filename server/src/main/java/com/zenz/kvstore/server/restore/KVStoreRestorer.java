package com.zenz.kvstore.server.restore;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.*;
import com.zenz.kvstore.server.snapshot.*;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class KVStoreRestorer {

    public KVStore restore(final KVStore kvStore) throws IOException {
        applySnapshots(kvStore);
        applyLogEntries(kvStore);
        return kvStore;
    }

    protected KVStore createStore() throws IOException {
        final KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter =
                new KVStoreSnapshotter<>(SingleSnapshotHeader.class, SingleSnapshotBody.class, SingleSnapshotFooter.class);
        final LogHandler logHandler = new LogHandler(
                new WALogger(Files.createTempFile("temp-logs", ".log")), snapshotter);
        return new KVStore(logHandler);
    }

    protected void applySnapshots(final KVStore kvStore) throws IOException {
        final BaseLogHandler<?, ?> logHandler = kvStore.getLogHandler();
        final KVStoreSnapshotter<?, ?, ?> snapshotter = logHandler.getSnapshotter();
        final Path snapshotDir = snapshotter.getDir();

        final File[] snapshotFiles = snapshotDir.toFile().listFiles();

        final CommandLogger prevLogger = logHandler.getLogger();
        logHandler.setLogger(new NoOpLogger());

        if (snapshotFiles != null) {
            final Path prevDir = kvStore.getLogHandler().getSnapshotter().getDir();
            kvStore.getLogHandler().getSnapshotter().setDir(Files.createTempDirectory("temp-snapshots-"));
            for (File snapshotFile : snapshotFiles) {
                final SnapshotBody body = snapshotter.getBody(snapshotFile.toPath());
                for (LogEntry logEntry : body.getEntries()) {
                    applyLogEntry(logEntry, kvStore, logHandler);
                }
            }
            kvStore.getLogHandler().getSnapshotter().setDir(prevDir);
        }

        logHandler.setLogger(prevLogger);
    }

    protected List<? extends LogEntry> applyLogEntries(final KVStore kvStore) throws IOException {
        final BaseLogHandler<?, ?> logHandler = kvStore.getLogHandler();
        final List<? extends LogEntry> logEntries = logHandler.loadLogs(kvStore.getLogHandler().getLogger().getPath());

        final CommandLogger prevLogger = logHandler.getLogger();
        logHandler.setLogger(new NoOpLogger());

        for (LogEntry logEntry : logEntries) {
            applyLogEntry(logEntry, kvStore, logHandler);
        }

        logHandler.setLogger(prevLogger);
        return logEntries;
    }

    protected void applyLogEntry(
            final LogEntry logEntry,
            final KVStore kvStore,
            final BaseLogHandler<?, ?> logHandler) throws IOException {
        logHandler.setLogId(logEntry.id - 1);

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
