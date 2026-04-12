package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.util.List;

public interface Creator<T, L extends LogEntry> {

    T create(List<L> entries);
}
