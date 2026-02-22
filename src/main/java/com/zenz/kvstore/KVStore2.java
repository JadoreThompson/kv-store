package com.zenz.kvstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.zenz.kvstore.operations.GetOperation;
import com.zenz.kvstore.operations.Operation;
import com.zenz.kvstore.operations.PutOperation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class KVStore2 {
    // Default settings
    private static final Path DEFAULT_LOGS_FOLDER = Path.of("logs");
    //    public static final int DEFAULT_LOGS_PER_SNAPSHOT = 1_000_000;
    public static final int DEFAULT_LOGS_PER_SNAPSHOT = 10;

    // Durability settings
    private KVMapSnapshotter snapshotter;
    private boolean snapshotEnabled;
    private final Path logsFolder;
    private WALogger logger;
    private boolean loggingEnabled;
    private int logCount;
    private int logsPerSnapshot;

    private final KVMap map;
    private final Random random;


    private KVStore2() throws IOException {
        logsFolder = DEFAULT_LOGS_FOLDER;
        snapshotter = new KVMapSnapshotter();
        snapshotEnabled = false;
        logCount = 0;
        map = new KVMap();
        random = new Random();
        logsPerSnapshot = DEFAULT_LOGS_PER_SNAPSHOT;
        loggingEnabled = true;
        configureLogger();
    }

//
//    private KVStore2(Path logsFolder) throws IOException {
//        this.logsFolder = logsFolder;
//        snapshotter = new KVMapSnapshotter();
//        snapshotEnabled = false;
//        logCount = 0;
//        map = new KVMap();
//        random = new Random();
//        configureLogger();
//    }

    private KVStore2(KVMap map) throws IOException {
        logsFolder = DEFAULT_LOGS_FOLDER;
        snapshotter = new KVMapSnapshotter();
        snapshotEnabled = true;
        logCount = 0;
        this.map = map;
        random = new Random();
        logsPerSnapshot = DEFAULT_LOGS_PER_SNAPSHOT;
        loggingEnabled = true;

        configureLogger();
    }

    private KVStore2(Path logsFolder) throws IOException {
        this.logsFolder = logsFolder;
        snapshotter = new KVMapSnapshotter();
        snapshotEnabled = true;
        logCount = 0;
        map = new KVMap();
        random = new Random();
        logsPerSnapshot = DEFAULT_LOGS_PER_SNAPSHOT;
        loggingEnabled = true;
        configureLogger();
    }

    private KVStore2(Path logsFolder, KVMap map) throws IOException {
        this.logsFolder = logsFolder;
        snapshotter = new KVMapSnapshotter();
        snapshotEnabled = true;
        logCount = 0;
        this.map = map;
        random = new Random();
        logsPerSnapshot = DEFAULT_LOGS_PER_SNAPSHOT;
        configureLogger();
    }

    private KVStore2(Builder builder) throws IOException {
        snapshotter = builder.snapshotter;
        snapshotEnabled = builder.snapshotEnabled;
        logsFolder = builder.logsFolder;
        logsPerSnapshot = builder.logsPerSnapshot;
        loggingEnabled = builder.loggingEnabled;
        map = builder.map;
        random = new Random();
        configureLogger();
    }

//    public KVStore2 load(Path logsFolderPath) throws IOException {
//        KVMap map = snapshotter.loadSnapshot();
//        Path snapshotFolderPath = snapshotter.getFolderPath();
//
//        File[] files = snapshotFolderPath.toFile().listFiles();
//        if (files == null || files.length == 0) return new KVStore2();
//
//        Path recentSnapshotFpath = null;
//        for (File f : files) {
//            recentSnapshotFpath = f.toPath();
//        }
//
//
//        // Applying each batch of logs. If a log needs snapshotting
//        // the store will trigger the snapshot
//        KVStore2 store = new KVStore2(map);
//
//        files = logsFolderPath.toFile().listFiles();
//        String recentSnapshotFname = recentSnapshotFpath.toFile().getName();
//        boolean reached = false;
//
//        for (File file : files) {
//            if (!reached) {
//                if (file.getName().equals(recentSnapshotFname)) {
//                    reached = true;
//                }
//            } else {
//                applyLogs(file, store);
//            }
//        }
//
//        return store;
//    }

//    public KVStore2 load() throws IOException {
//        KVStore2 store = new KVStore2();
//        handleLoadStore(store);
//        return store;
//    }

    public static KVStore2 load() throws IOException {
        KVMapSnapshotter snapshotter = new KVMapSnapshotter();
        KVMap map = snapshotter.loadSnapshot();

        KVStore2 store;
        if (map != null) {
            store = new KVStore2(map);
        } else {
            store = new KVStore2();
        }

        store.setLoggingEnabled(false);
        restoreState(store, snapshotter);
        return store;
    }

//    public KVStore2 load(Path logsFolder) throws IOException {
//        KVStore2 store = new KVStore2(logsFolder);
//        handleLoadStore(store);
//        return store;
//    }

    public static KVStore2 load(Path logsFolder) throws IOException {
        KVMapSnapshotter snapshotter = new KVMapSnapshotter();
        KVMap map = snapshotter.loadSnapshot();

        KVStore2 store;
        if (map != null) {
            store = new KVStore2(logsFolder, map);
        } else {
            store = new KVStore2(logsFolder);
        }

        store.setLoggingEnabled(false);
        restoreState(store, snapshotter);
        return store;
    }

    public static KVStore2 load(Path logsFolder, KVMapSnapshotter snapshotter) throws IOException {
        KVMap map = snapshotter.loadSnapshot();

        KVStore2 store;
        if (map != null) {
            store = new KVStore2(logsFolder, map);
        } else {
            store = new KVStore2(logsFolder);
        }

        store.setSnapshotter(snapshotter);
        store.setLoggingEnabled(false);
        restoreState(store, snapshotter);
        store.setLoggingEnabled(true);
        return store;
    }

//    public static KVStore2 load(String logsFolder) throws IOException {}

//    public KVStore2 load(KVMap map) throws IOException {
//        KVStore2 store = new KVStore2(map);
//        handleLoadStore(store);
//        return store;
//    }
//
//    public KVStore2 load(Path logsFolder, KVMap map) throws IOException {
//        KVStore2 store = new KVStore2(logsFolder, map);
//        handleLoadStore(store);
//        return store;
//    }

    public static KVStore2 load(Builder builder) throws IOException {
        KVMapSnapshotter snapshotter = (builder.snapshotter != null) ? builder.snapshotter : new KVMapSnapshotter();
        KVMap map = snapshotter.loadSnapshot();

        if (map != null) {
            builder.setMap(map);
        }

        KVStore2 store = new KVStore2(builder);
        store.setLoggingEnabled(false);
        restoreState(store, snapshotter);
        store.setLoggingEnabled(true);
        return store;
    }

    private void handleLoadStore(KVStore2 store) throws IOException {
        KVMap map = snapshotter.loadSnapshot();
        Path snapshotFolderPath = snapshotter.getFolderPath();

        File[] files = snapshotFolderPath.toFile().listFiles();
        if (files == null || files.length == 0) return;

        Path recentSnapshotFpath = null;
        for (File f : files) {
            recentSnapshotFpath = f.toPath();
        }


        // Applying each batch of logs. If a log needs snapshotting
        // the store will trigger the snapshot
        files = store.getLogsFolder().toFile().listFiles();
        String recentSnapshotFname = recentSnapshotFpath.toFile().getName();
        boolean reached = false;

        for (File file : files) {
            if (!reached) {
                if (file.getName().equals(recentSnapshotFname)) {
                    reached = true;
                }
            } else {
                applyLogs(file, store);
            }
        }
    }

    private static void restoreState(KVStore2 store, KVMapSnapshotter snapshotter) throws IOException {
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

    private static int applyLogs(File file, KVStore2 store) throws IOException {
        String contents = Files.readString(file.toPath());
        if (contents.length() == 0) return 0;

        String[] lines = contents.strip().split("\n");

        for (String line : lines) {
            Operation operation = Operation.fromLine(line);
            if (operation.type().equals(OperationType.PUT)) {
                PutOperation putOperation = (PutOperation) operation;
                store.put(putOperation.key(), putOperation.value());
            } else if (operation.type().equals(OperationType.GET)) {
                GetOperation getOperation = (GetOperation) operation;
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
        if (loggingEnabled) logger.logPut(generateId(), OperationType.PUT, key, value);
        logCount++;

        snapshot();

        map.put(key, value);
    }

    public KVMap.Node get(String key) throws IOException {
        if (loggingEnabled) logger.logGet(generateId(), OperationType.GET, key);
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
                Path fpath = Path.of(numFiles + ".log");
//                Path fpath = logsFolder.resolve(numFiles + ".log");
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

        public void setSnapshotter(KVMapSnapshotter snapshotter) {
            this.snapshotter = snapshotter;
        }

        public void setSnapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
        }

        public void setLogsFolder(Path logsFolder) {
            this.logsFolder = logsFolder;
        }

        public void setLoggingEnabled(boolean loggingEnabled) {
            this.loggingEnabled = loggingEnabled;
        }

        public void setLogsPerSnapshot(int logsPerSnapshot) {
            this.logsPerSnapshot = logsPerSnapshot;
        }

        public void setMap(KVMap map) {
            this.map = map;
        }

        public KVStore2 build() throws IOException {
//            return new KVStore2(this);
            return load(this);
        }
    }
}

