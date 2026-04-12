package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;

import java.util.List;

public class RaftSnapshotFooterCreator implements Snapshot.FooterCreator<RaftLogEntry> {

    static {
        SnapshotRegistry.registerFooterCreator(RaftSnapshotFooter.class, new RaftSnapshotFooterCreator());
    }

    @Override
    public RaftSnapshotFooter create(final List<RaftLogEntry> entries) {
        return new RaftSnapshotFooter(System.currentTimeMillis());
    }
}