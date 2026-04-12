package com.zenz.kvstore.server.restore;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.BaseLogHandler;
import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.snapshot.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class KVStoreRestorer {

    public KVStore restore() throws IOException {
        final KVStore kvStore = createStore();

        final Path dir = kvStore.getLogHandler().getSnapshotter().getDir();
        kvStore.getLogHandler().getSnapshotter().setDir(Files.createTempDirectory("temp-snapshots-"));
        applySnapshots(kvStore);

        kvStore.getLogHandler().getSnapshotter().setDir(dir);
        applyLogEntries(kvStore);

        return kvStore;
    }

    protected KVStore createStore() throws IOException {
        final KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter =
                new KVStoreSnapshotter<>(RaftSnapshotHeader.class, RaftSnapshotBody.class, RaftSnapshotFooter.class);
        final RaftLogHandler logHandler = new RaftLogHandler(
                new WALogger(Files.createTempFile("temp-logs", ".log")), snapshotter);
        return new KVStore(logHandler);
    }

    protected void applySnapshots(final KVStore kvStore) throws IOException {
        final BaseLogHandler<?, ?> logHandler = kvStore.getLogHandler();
        final KVStoreSnapshotter<?, ?, ?> snapshotter = logHandler.getSnapshotter();
        final Path snapshotDir = snapshotter.getDir();
        final File[] snapshotFiles = snapshotDir.toFile().listFiles();

        if (snapshotFiles != null) {
            for (File snapshotFile : snapshotFiles) {
                final Snapshot.Body body = snapshotter.getBody(snapshotFile.toPath());
                for (LogEntry logEntry : body.getEntries()) {
                    applyLogEntry(logEntry, kvStore, logHandler);
                }
            }
        }
    }

    protected List<? extends LogEntry> applyLogEntries(final KVStore kvStore) throws IOException {
        final BaseLogHandler<?, ?> logHandler = kvStore.getLogHandler();
        final List<? extends LogEntry> logEntries = logHandler.loadLogs(WALogger.DEFAULT_PATH);

        for (LogEntry logEntry : logEntries) {
            applyLogEntry(logEntry, kvStore, logHandler);
        }

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
}
