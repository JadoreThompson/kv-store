package com.zenz.kvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface CommandProcessor {
    public CompletableFuture<ByteBuffer> handleGet(String key) throws IOException;

    public CompletableFuture<ByteBuffer> handlePut(String key, byte[] value) throws IOException;
}
