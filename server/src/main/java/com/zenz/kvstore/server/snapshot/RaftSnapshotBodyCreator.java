package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;

import java.util.List;

public class RaftSnapshotBodyCreator implements Snapshot.BodyCreator<RaftLogEntry> {

    static {
        SnapshotRegistry.registerBodyCreator(RaftSnapshotBody.class, new RaftSnapshotBodyCreator());
    }

    @Override
    public RaftSnapshotBody create(final List<RaftLogEntry> entries) {
        return new RaftSnapshotBody(entries);
    }
}