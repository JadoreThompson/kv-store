package com.zenz.kvstore;

import com.zenz.kvstore.operations.PutOperation;
import com.zenz.kvstore.operations.RaftGetOperation;
import com.zenz.kvstore.operations.RaftPutOperation;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KVRaftStore extends KVStore {
    protected long logCount;
    protected long logId;
    protected long term;

    protected KVRaftStore(Builder builder) throws IOException {
        super(builder);
    }

    public void put(String key, byte[] value) throws IOException {
        if (loggingEnabled) logger.log(ByteBuffer.wrap(new RaftPutOperation(logId++, term, key, value).serialize()));
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

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }
}
