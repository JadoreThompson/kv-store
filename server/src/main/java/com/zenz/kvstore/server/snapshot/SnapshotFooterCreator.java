package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

public interface SnapshotFooterCreator<L extends LogEntry> extends Creator<SnapshotFooter, L> {
}
