package com.zenz.kvstore.server.restorer;

import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.server.KVMap;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handler.RaftLogHandler;

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
            ArrayList<RaftLogHandler.Log> logs = logHandler.deserialize(path);
            if (!logs.isEmpty() && logs.get(logs.size() - 1).id() == lastLogId) {
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

            if (log.term() != logHandler.getTerm()) logHandler.setTerm(log.term());
            // log handler increments each time
            logHandler.setLogId(log.id() - 1);

            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
            }
        }
    }
}
