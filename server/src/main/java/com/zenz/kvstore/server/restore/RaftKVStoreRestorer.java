package com.zenz.kvstore.server.restore;

import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotBody;
import com.zenz.kvstore.server.snapshot.RaftSnapshotFooter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotHeader;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class RaftKVStoreRestorer extends KVStoreRestorer {

    @Override
    public KVStore restore(final KVStore kvStore) throws IOException {
        applySnapshots(kvStore);
        final List<RaftLogEntry> logEntries = (List<RaftLogEntry>) applyLogEntries(kvStore);

        if (!logEntries.isEmpty()) {
            ((RaftLogHandler) kvStore.getLogHandler()).setTerm(logEntries.getLast().term);
        }

        return kvStore;
    }

    @Override
    protected KVStore createStore() throws IOException {
        final KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter =
                new KVStoreSnapshotter<>(RaftSnapshotHeader.class, RaftSnapshotBody.class, RaftSnapshotFooter.class);
        final RaftLogHandler logHandler = new RaftLogHandler(
                new WALogger(Files.createTempFile("temp-logs", ".log")), snapshotter);
        return new KVStore(logHandler);
    }
}
