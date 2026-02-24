package com.zenz.kvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * A CommandProcessor implementation that directly applies operations
 * to the underlying KVStore without any consensus mechanism.
 * 
 * This is used for single-node mode where no replication is needed.
 * All operations are synchronous and return immediately completed futures.
 */
public class DirectCommandProcessor implements CommandProcessor {
    private final KVStore store;

    public DirectCommandProcessor(KVStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<ByteBuffer> handleGet(String key) throws IOException {
        KVMap.Node node = store.get(key);

        String response;
        if (node == null) {
            response = "NULL";
        } else {
            response = "OK " + new String(node.value, StandardCharsets.UTF_8);
        }

        // Immediately complete the future (no consensus needed)
        return CompletableFuture.completedFuture(
            ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Override
    public CompletableFuture<ByteBuffer> handlePut(String key, byte[] value) throws IOException {
        store.put(key, value);

        // Immediately complete the future (no consensus needed)
        return CompletableFuture.completedFuture(
            ByteBuffer.wrap("OK".getBytes(StandardCharsets.UTF_8))
        );
    }
}