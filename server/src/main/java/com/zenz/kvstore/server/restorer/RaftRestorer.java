package com.zenz.kvstore.server.restorer;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.server.KVMap;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.logging.WALogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class RaftRestorer implements BaseRestorer {
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
        KVMap map = snapshotter.loadSnapshot();
        RaftLogHandler logHandler = (RaftLogHandler) builder.getLogHandler();

        if (map != null) {
            builder.setMap(map);

            Path snapshotDir = snapshotter.getDir();
            String prefix = snapshotDir.toFile().listFiles()[0].getName().replace(".snapshot", "");
            String[] parts = prefix.split("_");
            long lastLogId = Long.parseLong(parts[0]);
            long lastTerm = Long.parseLong(parts[1]);
            logHandler.setLogId(lastLogId);
            logHandler.setTerm(lastTerm);

            Path path = logHandler.getLogger().getPath();
            ArrayList<RaftLogHandler.LogEntry> logEntries = logHandler.deserialize(path);
            if (!logEntries.isEmpty() && logEntries.get(logEntries.size() - 1).Id() == lastLogId) {
                WALogger logger = logHandler.getLogger();
                path = logger.getPath();
                Files.deleteIfExists(path);
                Files.createFile(path);
                logHandler.setLogger(new WALogger(path));
            }
        }

        KVStore store = new KVStore(builder);
        logHandler.setEnabled(true);
        restoreState(store, logHandler);
        logHandler.setEnabled(false);
        return store;
    }

    private void restoreState(KVStore store, RaftLogHandler logHandler) throws IOException {
        ArrayList<RaftLogHandler.LogEntry> logEntries = logHandler.deserialize(logHandler.getLogger().getPath());
        if (logEntries == null || logEntries.isEmpty()) return;

        for (RaftLogHandler.LogEntry logEntry : logEntries) {
            Command command = logEntry.command();

            if (logEntry.term() != logHandler.getTerm()) logHandler.setTerm(logEntry.term());
            // logEntry handler increments each time
            logHandler.setLogId(logEntry.Id() - 1);

            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
            }
        }
    }
}
