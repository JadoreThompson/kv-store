package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.util.List;

public class SingleSnapshotBodyCreator implements Snapshot.BodyCreator<LogEntry> {

    static {
        SnapshotRegistry.registerBodyCreator(SingleSnapshotBody.class, new SingleSnapshotBodyCreator());
    }

    @Override
    public SingleSnapshotBody create(final List<LogEntry> entries) {
        return new SingleSnapshotBody(entries);
    }
}