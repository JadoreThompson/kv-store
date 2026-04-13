package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.util.ByteArraySerializable;

import java.util.List;

public interface SnapshotBody extends ByteArraySerializable {

    List<? extends LogEntry> getEntries();
}
