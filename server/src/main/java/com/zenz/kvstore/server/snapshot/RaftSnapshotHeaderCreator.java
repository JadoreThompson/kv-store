package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.RaftLogEntry;

import java.util.List;

public class RaftSnapshotHeaderCreator implements SnapshotHeaderCreator<RaftLogEntry> {

    private static final long VERSION = 1L;

    @Override
    public RaftSnapshotHeader create(final List<RaftLogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("Cannot create RaftSnapshotHeader from empty entries");
        }

        final RaftLogEntry firstEntry = entries.getFirst();
        final RaftLogEntry lastEntry = entries.getLast();

        return new RaftSnapshotHeader(
                VERSION,
                firstEntry.getId(),
                firstEntry.getTerm(),
                lastEntry.getId(),
                lastEntry.getTerm()
        );
    }
}