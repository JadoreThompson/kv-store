package com.zenz.kvstore;

import com.zenz.kvstore.operations.RaftGetOperation;
import com.zenz.kvstore.operations.RaftOperation;
import com.zenz.kvstore.operations.RaftPutOperation;
import com.zenz.kvstore.raft.KVRaftController;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class KVRaftControllerCommandProcessor implements CommandProcessor {
    private final Executor executor;
    private final KVRaftStore store;
    private final KVRaftController raft;

    public KVRaftControllerCommandProcessor(KVRaftStore store, KVRaftController raft) {
        executor = Executors.newFixedThreadPool(3);
        this.store = store;
        this.raft = raft;
    }

    @Override
    public CompletableFuture<ByteBuffer> handlePut(String key, byte[] value) throws IOException {
        store.put(key, value);

        long nextId = store.getLogId() + 1;
        RaftOperation raftOperation = new RaftPutOperation(nextId, store.getTerm(), key, value);
        executor.execute(() -> raft.handleCommand(raftOperation));

        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        future.complete(ByteBuffer.wrap("OK".getBytes(StandardCharsets.UTF_8)));
        return future;
    }

    @Override
    public CompletableFuture<ByteBuffer> handleGet(String key) throws IOException {
        KVMap.Node node = store.get(key);

        long nextId = store.getLogId() + 1;
        RaftOperation raftOperation = new RaftGetOperation(nextId, store.getTerm(), key);
        executor.execute(() -> raft.handleCommand(raftOperation));

        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();

        ByteBuffer result = ByteBuffer.allocate(3 + node.value.length);
        result.put("OK ".getBytes(StandardCharsets.UTF_8));
        result.put(node.value);

        future.complete(result);
        return future;
    }
}
