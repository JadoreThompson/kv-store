package com.zenz.kvstore;

import com.zenz.kvstore.commands.GetCommand;
import com.zenz.kvstore.commands.Command;
import com.zenz.kvstore.commands.PutCommand;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class KVStore {
    // Default settings
    private static final Path DEFAULT_LOGS_FOLDER = Path.of("logs");
    public static final int DEFAULT_LOGS_PER_SNAPSHOT = 100_000;

    // Durability settings
    private KVMapSnapshotter snapshotter;
    private boolean snapshotEnabled;
    private final Path logsFolder;
    private WALogger logger;
    private boolean loggingEnabled;
    private int logCount;
    private int logsPerSnapshot;

    private final KVMap map;
    private final Random random = new Random();

    private KVStore(Builder builder) throws IOException {
        snapshotter = builder.snapshotter;
        snapshotEnabled = builder.snapshotEnabled;
        logsFolder = (builder.logsFolder == null) ? DEFAULT_LOGS_FOLDER : builder.logsFolder;
        logsPerSnapshot = (builder.logsPerSnapshot <= 0) ? DEFAULT_LOGS_PER_SNAPSHOT : builder.logsPerSnapshot;
        loggingEnabled = builder.loggingEnabled;
        map = (builder.map == null) ? new KVMap() : builder.map;
        configureLogger();
    }

    private static KVStore load(Builder builder) throws IOException {
        KVMapSnapshotter snapshotter = (builder.snapshotter != null) ? builder.snapshotter : new KVMapSnapshotter();
        KVMap map = snapshotter.loadSnapshot();

        if (map != null) {
            builder.setMap(map);
        }

        KVStore store = new KVStore(builder);
        store.setLoggingEnabled(false);
        restoreState(store, snapshotter);
        store.setLoggingEnabled(true);
        return store;
    }

    private static void restoreState(KVStore store, KVMapSnapshotter snapshotter) throws IOException {
        Path snapshotFolderPath = snapshotter.getFolderPath();

        File[] files = snapshotFolderPath.toFile().listFiles();

        Path recentSnapshotFpath = null;
        for (File f : files) {
            recentSnapshotFpath = f.toPath();
        }

        // Applying each batch of logs. If a log needs snapshotting
        // the store will trigger the snapshot
        files = store.getLogsFolder().toFile().listFiles();
        String recentSnapshotFname = (recentSnapshotFpath != null) ? recentSnapshotFpath.toFile().getName() : null;
        boolean reached = false;

        for (File file : files) {
            if (!reached && recentSnapshotFname != null) {
                if (file.getName().equals(recentSnapshotFname)) {
                    reached = true;
                }
            } else {
                applyLogs(file, store);
            }
        }
    }

    private static int applyLogs(File file, KVStore store) throws IOException {
        String contents = Files.readString(file.toPath());
        if (contents.length() == 0) return 0;

        String[] lines = contents.strip().split("\n");

        for (String line : lines) {
            Command operation = Command.deserialize(line);
            if (operation.type().equals(CommandType.PUT)) {
                PutCommand putOperation = (PutCommand) operation;
                store.put(putOperation.key(), putOperation.value());
            } else if (operation.type().equals(CommandType.GET)) {
                GetCommand getOperation = (GetCommand) operation;
                store.get(getOperation.key());
            } else {
                throw new UnsupportedEncodingException("Unsupported operation " + operation.type().getValue());
            }
        }

        return lines.length;
    }

    private void configureLogger() throws IOException {
        File logsFolderFile = logsFolder.toFile();
        if (!logsFolderFile.exists()) {
            logsFolderFile.mkdirs();
        }

        // Get list of existing log files
        File[] logFiles = logsFolderFile.listFiles();
        long numFiles = (logFiles != null) ? logFiles.length : 0;

        Path logFpath;
        if (numFiles == 0) {
            logFpath = logsFolder.resolve("0.log");
            logFpath.toFile().createNewFile();
        } else {
            // Find the most recent log file
            String recentFName = (numFiles - 1) + ".log";
            logFpath = logsFolder.resolve(recentFName);
        }

        logger = new WALogger(logFpath.toString());
    }

    public void put(String key, byte[] value) throws IOException {
        if (loggingEnabled) logger.logPut(generateId(), CommandType.PUT, key, value);
        logCount++;

        snapshot();

        map.put(key, value);
    }

    public KVMap.Node get(String key) throws IOException {
        if (loggingEnabled) logger.logGet(generateId(), CommandType.GET, key);
        logCount++;

        snapshot();

        KVMap.Node result = map.get(key);
        return result;
    }

    private int generateId() {
        return random.nextInt();
    }

    private void snapshot() throws IOException {
        if (logCount >= logsPerSnapshot) {
            logCount = 0;

            if (snapshotEnabled) {
                snapshotter.snapshot(map);
                // Create a new log file
                File[] logFiles = logsFolder.toFile().listFiles();
                long numFiles = logFiles.length;
                Path fpath = logsFolder.resolve(numFiles + ".log");
                File file = fpath.toFile();
                if (!file.exists()) file.createNewFile();
                logger = new WALogger(fpath.toString());
            }
        }
    }

    /**
     * @return KVStore instance from the latest snapshot. Any logs
     * written after the snapshot will be applied.
     */
    public Path getLogsFolder() {
        return logsFolder;
    }

    public boolean isLoggingEnabled() {
        return loggingEnabled;
    }

    public void setLoggingEnabled(boolean enabled) {
        loggingEnabled = enabled;
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

    public static class Builder {
        private KVMapSnapshotter snapshotter;
        private boolean snapshotEnabled;
        private Path logsFolder;
        private boolean loggingEnabled;
        private int logsPerSnapshot;
        private KVMap map;

        public Builder() throws IOException {
            snapshotter = null;
            snapshotEnabled = true;
            logsFolder = null;
            loggingEnabled = true;
            logsPerSnapshot = 0;
            map = null;
        }

        public Builder setSnapshotter(KVMapSnapshotter snapshotter) {
            this.snapshotter = snapshotter;
            return this;
        }

        public Builder setSnapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
            return this;
        }

        public Builder setLogsFolder(Path logsFolder) {
            this.logsFolder = logsFolder;
            return this;
        }

        public Builder setLoggingEnabled(boolean loggingEnabled) {
            this.loggingEnabled = loggingEnabled;
            return this;
        }

        public Builder setLogsPerSnapshot(int logsPerSnapshot) {
            this.logsPerSnapshot = logsPerSnapshot;
            return this;
        }

        public Builder setMap(KVMap map) {
            this.map = map;
            return this;
        }

        public KVStore build() throws IOException {
            return load(this);
        }
    }
}

