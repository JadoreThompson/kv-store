package com.zenz.kvstore.server;

import com.zenz.kvstore.common.commands.DeleteCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handler.BaseLogHandler;
import com.zenz.kvstore.server.logging.handler.RaftLogHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class KVStore {
    public static final int DEFAULT_LOGS_PER_SNAPSHOT = 100_000;

    private KVMapSnapshotter snapshotter;
    private boolean snapshotEnabled;
    private BaseLogHandler logHandler;
    private int logCount;
    private int logsPerSnapshot;

    private final KVMap map;
    private final Trie trie = new Trie('\0');
    private boolean isRaftMode;

    public KVStore(Builder builder) throws IOException {
        this.snapshotter = builder.snapshotter;
        this.snapshotEnabled = builder.snapshotEnabled;
        this.logHandler = builder.logHandler;
        this.logsPerSnapshot = builder.logsPerSnapshot;
        this.map = (builder.map == null) ? new KVMap() : builder.map;
        this.isRaftMode = builder.isRaftMode;
    }

    public void put(String key, byte[] value) throws IOException {
        this.logHandler.log(new PutCommand(key, value));
        this.logCount++;

        snapshot();
        this.map.put(key, value);
        this.trie.add(key);
    }

    public KVMap.Node get(String key) {
        return this.map.get(key);
    }

    public boolean delete(String key) throws IOException {
        this.logCount++;
        this.logHandler.log(new DeleteCommand(key));
        snapshot();

        if (this.map.remove(key)) {
            this.trie.remove(key);
            return true;
        }

        return false;
    }

    /**
     * Returns all nodes whose keys share the prefix
     *
     * @param prefix
     * @return
     */
    public List<KVMap.Node> search(final String prefix) {
        final List<String> keys = this.trie.search(prefix);
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

        return keys.stream().map(this::get).toList();
    }

    private void snapshot() throws IOException {
        if (this.logCount >= this.logsPerSnapshot) {
            this.logCount = 0;

            if (this.snapshotEnabled) {
                // Create snapshot and move to main
                Path snapshotDir = this.snapshotter.getDir();
                Path fpath;
                if (this.isRaftMode) {
                    fpath = snapshotDir.resolve(
                            this.logHandler.getLogId() +
                                    "_" + ((RaftLogHandler) this.logHandler).getTerm() +
                                    ".snapshot"
                    );
                } else {
                    fpath = snapshotDir.resolve(this.logHandler.getLogId() + ".snapshot");
                }
                this.snapshotter.snapshot(this.map, fpath);
                for (File file : snapshotDir.toFile().listFiles()) {
                    Path fp = file.toPath();
                    if (!fp.equals(fpath)) Files.delete(fp);
                }

                // Creating new log file
                WALogger logger = this.logHandler.getLogger();
                Path path = logger.getPath();
                Files.deleteIfExists(path);
                Files.createFile(path);
                this.logHandler.setLogger(new WALogger(path));
            }
        }
    }

    public boolean isSnapshotEnabled() {
        return this.snapshotEnabled;
    }

    public void setSnapshotEnabled(boolean enabled) {
        this.snapshotEnabled = enabled;
    }

    public void setSnapshotter(KVMapSnapshotter snapshotter) {
        this.snapshotter = snapshotter;
    }

    public KVMapSnapshotter getSnapshotter() {
        return this.snapshotter;
    }

    public void setLogsPerSnapshot(int logsPerSnapshot) {
        this.logsPerSnapshot = logsPerSnapshot;
    }

    public KVMap getMap() {
        return this.map;
    }

    public BaseLogHandler getLogHandler() {
        return this.logHandler;
    }

    public boolean isRaftMode() {
        return this.isRaftMode;
    }

    public static class Builder {
        private KVMapSnapshotter snapshotter = null;
        private boolean snapshotEnabled = true;
        private int logsPerSnapshot = DEFAULT_LOGS_PER_SNAPSHOT;
        private KVMap map = null;
        private BaseLogHandler logHandler = null;
        private boolean isRaftMode = false;

        public Builder() {
        }

        public KVMapSnapshotter getSnapshotter() {
            return this.snapshotter;
        }

        public Builder setSnapshotter(KVMapSnapshotter snapshotter) {
            this.snapshotter = snapshotter;
            return this;
        }

        public boolean getSnapshotEnabled() {
            return this.snapshotEnabled;
        }

        public Builder setSnapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
            return this;
        }

        public BaseLogHandler getLogHandler() {
            return this.logHandler;
        }

        public Builder setLogHandler(BaseLogHandler logHandler) {
            this.logHandler = logHandler;
            return this;
        }

        public int getLogsPerSnapshot() {
            return this.logsPerSnapshot;
        }

        public Builder setLogsPerSnapshot(int logsPerSnapshot) {
            this.logsPerSnapshot = logsPerSnapshot;
            return this;
        }

        public KVMap getMap() {
            return this.map;
        }

        public Builder setMap(KVMap map) {
            this.map = map;
            return this;
        }

        public boolean isRaftMode() {
            return this.isRaftMode;
        }

        public Builder setRaftMode(boolean isRaftMode) {
            this.isRaftMode = isRaftMode;
            return this;
        }
    }
}