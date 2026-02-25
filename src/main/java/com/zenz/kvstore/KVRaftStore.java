package com.zenz.kvstore;

import com.zenz.kvstore.operations.RaftGetOperation;
import com.zenz.kvstore.operations.RaftOperation;
import com.zenz.kvstore.operations.RaftPutOperation;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
            logId++;
            ByteBuffer src = ByteBuffer.wrap(new RaftPutOperation(logId, term, key, value).serialize());
            logger.log(src);
        }

        logCount++;
        snapshot();
        map.put(key, value);
    }

    public KVMap.Node get(String key) throws IOException {
        if (loggingEnabled) {
            logId++;
            ByteBuffer src = ByteBuffer.wrap(new RaftGetOperation(logId, term, key).serialize());
            logger.log(src);
        }

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
        ByteBuffer fileBuffer = ByteBuffer.wrap(Files.readAllBytes(file.toPath()));
        List<RaftOperation> operations = RaftOperation.parseRaftLogs(fileBuffer);

        for (RaftOperation operation : operations) {
            if (operation.type().equals(OperationType.PUT)) {
                RaftPutOperation op = (RaftPutOperation) operation;
                store.put(op.key(), op.value());
            } else if (operation.type().equals(OperationType.GET)) {
                RaftGetOperation op = (RaftGetOperation) operation;
                store.get(op.key());
            } else {
                throw new UnsupportedOperationException("Unsupported operation type: " + operation.type());
            }
        }

        return operations.size();
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
