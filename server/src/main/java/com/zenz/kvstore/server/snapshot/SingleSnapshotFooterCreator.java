package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.util.List;

public class SingleSnapshotFooterCreator implements Snapshot.FooterCreator<LogEntry> {

    static {
        SnapshotRegistry.registerFooterCreator(SingleSnapshotFooter.class, new SingleSnapshotFooterCreator());
    }

    @Override
    public SingleSnapshotFooter create(final List<LogEntry> entries) {
        return new SingleSnapshotFooter(System.currentTimeMillis());
    }
}