package com.zenz.kvstore;

import java.io.IOException;
import java.util.Random;

public class KVStore {
    private final KVMap map;
    private final Random random;
    public final WALogger logger;
    private int curCommandId;
    private int logsPerSnapshot;
    private int logCount;

    public KVStore() throws IOException {
        map = new KVMap();
        logger = new WALogger("wal.log");
        random = new Random();
    }

    public KVStore(WALogger logger) {
        map = new KVMap();
        random = new Random();
        this.logger = logger;
    }

    public void put(String key, byte[] value) throws IOException {
        int commandId = getCommandId();
        logger.log(new WALogger.Log(commandId, WALogger.Operation.PUT, key + " " + value));
        logCount += 1;
        map.put(key, value);
        curCommandId = commandId;
    }

    public KVMap.Node get(String key) throws IOException {
        int commandId = getCommandId();
        logger.log(new WALogger.Log(commandId, WALogger.Operation.GET, key));
        logCount += 1;
        KVMap.Node result = map.get(key);
        curCommandId = commandId;
        return result;
    }

    private int getCommandId() {
        curCommandId = random.nextInt();
        return curCommandId;
    }

    public int getCurCommandId() {
        return curCommandId;
    }

    public void setLogsPerSnapshot(int logsPerSnapshot) {
        this.logsPerSnapshot = logsPerSnapshot;
    }

    public int getLogsPerSnapshot() {
        return logsPerSnapshot;
    }
}
