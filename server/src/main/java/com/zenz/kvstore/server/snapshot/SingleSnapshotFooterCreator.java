package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.util.List;

public class SingleSnapshotFooterCreator implements SnapshotFooterCreator<LogEntry> {

    @Override
    public SingleSnapshotFooter create(final List<LogEntry> entries) {
        return new SingleSnapshotFooter(System.currentTimeMillis());
    }
}