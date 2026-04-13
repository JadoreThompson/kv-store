package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

public interface SnapshotHeaderCreator<L extends LogEntry> extends Creator<SnapshotHeader, L> {
}
