package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.server.KVStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Snapshotter<L extends LogEntry> {

    void restore(KVStore kvstore);

    Path snapshot(List<L> entries) throws IOException;

    /**
     * @param entries Entries for the snapshot
     * @return Returns what would be file path for the snapshot
     */
    Path getFpath(List<L> entries);

    Path findSnapshot(long logId);
}
