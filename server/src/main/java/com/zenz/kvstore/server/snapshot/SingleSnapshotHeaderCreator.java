package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.util.List;

public class SingleSnapshotHeaderCreator implements Snapshot.HeaderCreator<LogEntry> {

    private static final long VERSION = 1L;

    static {
        SnapshotRegistry.registerHeaderCreator(SingleSnapshotHeader.class, new SingleSnapshotHeaderCreator());
    }

    @Override
    public SingleSnapshotHeader create(final List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("Cannot create SingleSnapshotHeader from empty entries");
        }

        final LogEntry firstEntry = entries.getFirst();
        final LogEntry lastEntry = entries.getLast();

        return new SingleSnapshotHeader(
                VERSION,
                firstEntry.getId(),
                lastEntry.getId()
        );
    }
}