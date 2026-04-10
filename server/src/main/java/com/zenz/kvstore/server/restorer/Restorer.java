package com.zenz.kvstore.server.restorer;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.LogHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Restorer implements BaseRestorer {
    /**
     * Restores a KVStore and passes the Builder to the constructor
     * of the KVStore.
     *
     * @param builder
     * @return
     * @throws Exception
     */
    @Override
    public KVStore restore(KVStore.Builder builder) throws Exception {
        KVMapSnapshotter snapshotter = (builder.getSnapshotter() != null) ? builder.getSnapshotter() : new KVMapSnapshotter();
        LogHandler logHandler = (LogHandler) builder.getLogHandler();
        final List<KVMapSnapshotter.KVPair> pairs = snapshotter.loadPairs();

        if (pairs != null) {
            Path snapshotDir = snapshotter.getDir();
            String prefix = snapshotDir.toFile().listFiles()[0].getName().replace(".snapshot", "");
            long lastLogId = Long.parseLong(prefix);
            logHandler.setLogId(lastLogId);

            Path path = logHandler.getLogger().getPath();
            ArrayList<LogHandler.LogEntry> logs = logHandler.deserialize(path);
            if (logs != null && !logs.isEmpty() && logs.get(logs.size() - 1).Id() == lastLogId) {
                WALogger logger = logHandler.getLogger();
                path = logger.getPath();
                Files.deleteIfExists(path);
                Files.createFile(path);
                logHandler.setLogger(new WALogger(path));
            }
        }

        KVStore store = new KVStore(builder);

        logHandler.setEnabled(true);
        if (pairs != null) {
            for (KVMapSnapshotter.KVPair pair : pairs) {
                store.put(pair.key(), pair.value());
            }
        }

        restoreState(store, logHandler);
        logHandler.setEnabled(false);
        return store;
    }

    private void restoreState(KVStore store, LogHandler logHandler) throws IOException {
        ArrayList<LogHandler.LogEntry> logs = logHandler.deserialize(logHandler.getLogger().getPath());
        if (logs == null || logs.isEmpty()) return;

        for (LogHandler.LogEntry log : logs) {
            Command command = log.command();
            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
            } else if (command.type().equals(CommandType.DELETE)) {
                DeleteCommand comm = (DeleteCommand) command;
                store.delete(comm.key());
            }
        }
    }
}
