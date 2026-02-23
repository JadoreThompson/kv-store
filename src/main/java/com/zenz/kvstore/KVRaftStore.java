package com.zenz.kvstore;

import com.zenz.kvstore.operations.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class KVRaftStore extends KVStore {
    protected static final Path DEFAULT_LOGS_FOLDER = Path.of("raft-logs");
    protected long logId;
    protected long term;

    protected KVRaftStore(Builder builder) throws IOException {
        super(builder);
        logId = 0;
    }

    public void put(String key, byte[] value) throws IOException {
        if (loggingEnabled) {
            ByteBuffer src = ByteBuffer.wrap(new RaftPutOperation(logId++, term, key, value).serialize());
            System.out.println("Within the put command we have " + src.remaining() + " bytes remaining");
            logger.log(src);
        }

        logCount++;
        snapshot();
        map.put(key, value);
    }

    public KVMap.Node get(String key) throws IOException {
        if (loggingEnabled) logger.log(ByteBuffer.wrap(new RaftGetOperation(logId++, term, key).serialize()));
        logCount++;
        snapshot();
        return map.get(key);
    }

    protected static KVRaftStore load(Builder builder) throws IOException {
        KVMapSnapshotter snapshotter = (builder.snapshotter != null) ? builder.snapshotter : new KVMapSnapshotter();
        KVMap map = snapshotter.loadSnapshot();

        if (map != null) {
            builder.setMap(map);
        }

        KVRaftStore store = new KVRaftStore(builder);
        store.setLoggingEnabled(false);
        System.out.println("Initiating the restoration process");
        restoreState(store, snapshotter);
        store.setLoggingEnabled(true);
        return store;
    }

    protected static void restoreState(KVRaftStore store, KVMapSnapshotter snapshotter) throws IOException {
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

    protected static int applyLogs(File file, KVRaftStore store) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(Files.readAllBytes(file.toPath()));
        if (bb.capacity() == 0) return 0;

        ByteBuffer curBuff = ByteBuffer.allocate(1024);
        for (byte b : bb.array()) {
            char ch = (char) b;

            if (ch != '\n') {
                curBuff.put(b);
                continue;
            }

            curBuff.flip();
            RaftOperation operation = RaftOperation.deserialize(curBuff);

            store.setLogId(operation.id());
            if (operation.term() != store.getTerm()) store.setTerm(operation.term());

            if (operation.type().equals(OperationType.PUT)) {
                RaftPutOperation putOperation = (RaftPutOperation) operation;
                store.put(putOperation.key(), putOperation.value());
            } else {
                RaftGetOperation getOperation = (RaftGetOperation) operation;
                store.get(getOperation.key());
            }

            curBuff.clear();
        }

        return 1;
    }

    public long getLogId() {
        return logId;
    }

    protected void setLogId(long logId) {
        this.logId = logId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public static class Builder extends KVStore.Builder<Builder> {
        public Builder() {
            super();
        }

        @Override
        public Builder self() {
            return this;
        }

        public KVRaftStore build() throws IOException {
            return load(this);
        }
    }
}
