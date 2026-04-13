package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.util.List;

public class SingleSnapshotBodyCreator implements SnapshotBodyCreator<LogEntry> {

    @Override
    public SingleSnapshotBody create(final List<LogEntry> entries) {
        return new SingleSnapshotBody(entries);
    }
}