package com.zenz.kvstore.server.restore;

import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;

import java.io.IOException;
import java.util.List;

public class RaftKVStoreRestorer extends KVStoreRestorer {

    @Override
    public KVStore restore(final KVStore kvStore) throws IOException {
        applySnapshots(kvStore);
        final List<RaftLogEntry> logEntries = (List<RaftLogEntry>) applyLogEntries(kvStore);
        if (!logEntries.isEmpty()) {
            ((RaftLogHandler) kvStore.getLogHandler()).setTerm(logEntries.getLast().term);
        }

        return kvStore;
    }
}
