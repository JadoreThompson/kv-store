package com.zenz.kvstore;

import com.zenz.kvstore.commands.PutCommand;
import com.zenz.kvstore.logHandlers.BaseLogHandler;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class KVStore {
    // Default settings
    public static final int DEFAULT_LOGS_PER_SNAPSHOT = 100_000;

    // Durability settings
    private KVMapSnapshotter snapshotter;
    private boolean snapshotEnabled;
    private BaseLogHandler logHandler;
    private int logCount;
    private int logsPerSnapshot;

    private final KVMap map;

    public KVStore(Builder builder) throws IOException {
        snapshotter = builder.snapshotter;
        snapshotEnabled = builder.snapshotEnabled;
        logHandler = builder.logHandler;
        logsPerSnapshot = builder.logsPerSnapshot;
        map = (builder.map == null) ? new KVMap() : builder.map;
    }


    public void put(String key, byte[] value) throws IOException {
        logHandler.log(new PutCommand(key, value));
        logCount++;

        snapshot();
        map.put(key, value);
    }

    public KVMap.Node get(String key) throws IOException {
        logCount++;

        snapshot();
        KVMap.Node result = map.get(key);
        return result;
    }

    private void snapshot() throws IOException {
        if (logCount >= logsPerSnapshot) {
            logCount = 0;

            if (snapshotEnabled) {
                // Create snapshot and move to main
                Path snapshotDir = snapshotter.getDir();
                Path fpath = snapshotDir.resolve(logHandler.getLogId() + ".snapshot");
                snapshotter.snapshot(map, fpath);
                for (File file : snapshotDir.toFile().listFiles()) {
                    Path fp = file.toPath();
                    if (!fp.equals(fpath)) Files.delete(fp);
                }

                // Creating new log file
                WALogger logger = logHandler.getLogger();
                Path path = logger.getPath();
                Files.deleteIfExists(path);
                Files.createFile(path);
                logHandler.setLogger(new WALogger(path));
            }
        }
    }

    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    public void setSnapshotEnabled(boolean enabled) {
        snapshotEnabled = enabled;
    }

    public void setSnapshotter(KVMapSnapshotter snapshotter) {
        this.snapshotter = snapshotter;
    }

    public KVMapSnapshotter getSnapshotter() {
        return snapshotter;
    }

    public void setLogsPerSnapshot(int logsPerSnapshot) {
        this.logsPerSnapshot = logsPerSnapshot;
    }

    public KVMap getMap() {
        return map;
    }

    public BaseLogHandler getLogHandler() {
        return logHandler;
    }

    public static class Builder {
        private KVMapSnapshotter snapshotter = null;
        private boolean snapshotEnabled = true;
        private int logsPerSnapshot = DEFAULT_LOGS_PER_SNAPSHOT;
        private KVMap map = null;
        private BaseLogHandler logHandler = null;

        public Builder() {
        }

        public KVMapSnapshotter getSnapshotter() {
            return snapshotter;
        }

        public Builder setSnapshotter(KVMapSnapshotter snapshotter) {
            this.snapshotter = snapshotter;
            return this;
        }

        public boolean getSnapshotEnabled() {
            return snapshotEnabled;
        }

        public Builder setSnapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
            return this;
        }

        public BaseLogHandler getLogHandler() {
            return logHandler;
        }

        public Builder setLogHandler(BaseLogHandler logHandler) {
            this.logHandler = logHandler;
            return this;
        }

        public int getLogsPerSnapshot() {
            return logsPerSnapshot;
        }

        public Builder setLogsPerSnapshot(int logsPerSnapshot) {
            this.logsPerSnapshot = logsPerSnapshot;
            return this;
        }

        public KVMap getMap() {
            return map;
        }

        public Builder setMap(KVMap map) {
            this.map = map;
            return this;
        }
    }
}

