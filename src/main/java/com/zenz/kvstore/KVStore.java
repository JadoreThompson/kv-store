package com.zenz.kvstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class KVStore {
    public static final int LOGS_PER_SNAPSHOT = 1_000_000;
    private final File snapshotFolder = new File("snapshots");

    private File logsFolder;
    private String logFname;
    public WALogger logger;
    private int logCount;
    private KVSnapshotter snapshotter;
    private final boolean snapshotEnabled;

    private final KVMap map;
    private final Random random;


    public KVStore(String folderPath, boolean snapshotEnabled) throws IOException {
        setWALogger(folderPath);
        map = new KVMap();
        random = new Random();
        this.snapshotEnabled = snapshotEnabled;
    }

    public KVStore(String folderPath, boolean snapshotEnabled, KVMap map) throws IOException {
        setWALogger(folderPath);
        this.map = map;
        random = new Random();
        this.snapshotEnabled = snapshotEnabled;
    }

    private void setWALogger(String folderPath) throws IOException {
        logsFolder = new File(folderPath);
        if (!logsFolder.exists()) {
            logsFolder.mkdirs();
        }

        // Get list of existing log files
        File[] logFiles = logsFolder.listFiles();
        long numFiles = (logFiles != null) ? logFiles.length : 0;

        if (numFiles == 0) {
            // First run - create initial log file
            logFname = folderPath + "/0.log";
            new File(logFname).createNewFile();
        } else {
            // Find the most recent log file
            System.out.println("All files found within logs folder +" + "'" + logsFolder.toString() + "'");
            for (File file : logFiles) {
                System.out.println("    - " + file.toString());
            }
            System.out.println();
            String recentFName = (numFiles - 1) + ".log";
            logFname = folderPath + "/" + recentFName;
            String contents = Files.readString(Path.of(logFname));
            String[] lines = contents.strip().split("\n");

            if (!contents.isEmpty() && lines.length >= LOGS_PER_SNAPSHOT) {
                // Create new file
                logFname = folderPath + "/" + numFiles + ".log";
                new File(logFname).createNewFile();
            }
        }

        logger = new WALogger(logFname);
    }

    public void put(String key, byte[] value) throws IOException {
        snapshot();

        logger.logPut(getCommandId(), OperationType.PUT, key, value);
        logCount++;

        map.put(key, value);
    }

    public KVMap.Node get(String key) throws IOException {
        snapshot();

        logger.logGet(getCommandId(), OperationType.GET, key);
        logCount++;

        KVMap.Node result = map.get(key);
        return result;
    }

    public KVMap getMap() {
        return map;
    }

    private int getCommandId() {
        return random.nextInt();
    }

    private void snapshot() throws IOException {
        if (snapshotEnabled && logCount >= LOGS_PER_SNAPSHOT) {
            snapshotter.snapshot(logFname);
        }
    }
}
