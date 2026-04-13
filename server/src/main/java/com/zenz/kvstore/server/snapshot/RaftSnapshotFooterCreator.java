package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;

import java.util.List;

public class RaftSnapshotFooterCreator implements SnapshotFooterCreator<RaftLogEntry> {

    @Override
    public RaftSnapshotFooter create(final List<RaftLogEntry> entries) {
        return new RaftSnapshotFooter(System.currentTimeMillis());
    }
}