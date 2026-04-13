package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

public interface SnapshotBodyCreator<L extends LogEntry> extends Creator<SnapshotBody, L> {
}
