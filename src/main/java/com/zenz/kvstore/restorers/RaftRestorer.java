package com.zenz.kvstore.restorers;

import com.zenz.kvstore.*;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.LogHandler;
import com.zenz.kvstore.logHandlers.RaftLogHandler;

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
            long lastLogId = Long.parseLong(prefix);
            logHandler.setLogId(lastLogId);

            Path path = logHandler.getLogger().getPath();
            ArrayList<RaftLogHandler.Log> logs = logHandler.deserialize(path);
            if (logs != null && !logs.isEmpty() && logs.get(logs.size() - 1).id() == lastLogId) {
                WALogger logger = logHandler.getLogger();
                path = logger.getPath();
                Files.deleteIfExists(path);
                Files.createFile(path);
                logHandler.setLogger(new WALogger(path));
            }
        }

        KVStore store = new KVStore(builder);
        logHandler.setDisabled(true);
        restoreState(store, logHandler);
        logHandler.setDisabled(false);
        return store;
    }

    private void restoreState(KVStore store, RaftLogHandler logHandler) throws IOException {
        ArrayList<RaftLogHandler.Log> logs = logHandler.deserialize(logHandler.getLogger().getPath());
        if (logs == null || logs.isEmpty()) return;

        for (RaftLogHandler.Log log : logs) {
            Command command = log.command();

            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
            }
        }
    }
}
