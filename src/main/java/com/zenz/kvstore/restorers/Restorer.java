package com.zenz.kvstore.restorers;

import com.zenz.kvstore.*;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.GetCommand;
import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.log_handlers.BaseLogHandler;
import com.zenz.kvstore.log_handlers.LogHandler;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;

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
        KVMap map = snapshotter.loadSnapshot();

        if (map != null) {
            builder.setMap(map);
        }

        KVStore store = new KVStore(builder);
        store.setLoggingEnabled(false);
        restoreState(store, snapshotter, builder);

        store.setLoggingEnabled(true);
        return null;
    }

    /**
     * Applies logs from each log file created after the snapshot
     * file was created
     *
     * @param store
     * @param snapshotter
     * @param builder
     * @throws IOException
     */
    private void restoreState(KVStore store, KVMapSnapshotter snapshotter, KVStore.Builder builder) throws IOException {
        Path snapshotFolderPath = snapshotter.getFolderPath();
        Path recentSnapshotFpath = null;
        File[] files = snapshotFolderPath.toFile().listFiles();
        for (File f : files) {
            recentSnapshotFpath = f.toPath();
        }

        // Applying each batch of logs. If a log needs snapshotting
        // the store will trigger the snapshot
        BaseLogHandler logHandler = builder.getLogHandler();
        files = logHandler.getLogDir().toFile().listFiles();
        String recentSnapshotFname = (recentSnapshotFpath != null) ? recentSnapshotFpath.toFile().getName() : null;
        boolean reached = false;
        for (File file : files) {
            if (!reached && recentSnapshotFname != null) {
                reached = file.getName().equals(recentSnapshotFname);
            } else {
                applyLogs(file.toPath(), store, builder);
            }
        }
    }

    /**
     * Deserialise each line within the log file
     * and performs the command with the store
     *
     * @param fpath
     * @param store
     * @param builder
     * @return
     * @throws IOException
     */
    private int applyLogs(Path fpath, KVStore store, KVStore.Builder builder) throws IOException {
        ArrayList<LogHandler.Log> logs = builder.getLogHandler().deserialize(fpath);
        if (logs == null || logs.size() == 0) return 0;

        for (LogHandler.Log log : logs) {
            Command command = log.command();
            if (command.type().equals(CommandType.PUT)) {
                PutCommand comm = (PutCommand) command;
                store.put(comm.key(), comm.value());
            } else if (command.type().equals(CommandType.GET)) {
                GetCommand comm = (GetCommand) command;
                store.get(comm.key());
            } else {
                throw new UnsupportedEncodingException("Unsupported operation " + command.type().getValue());
            }
        }

        return logs.size();
    }
}
