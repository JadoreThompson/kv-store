package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;

import java.util.List;

public class RaftSnapshotBodyCreator implements SnapshotBodyCreator<RaftLogEntry> {

    @Override
    public RaftSnapshotBody create(final List<RaftLogEntry> entries) {
        return new RaftSnapshotBody(entries);
    }
}