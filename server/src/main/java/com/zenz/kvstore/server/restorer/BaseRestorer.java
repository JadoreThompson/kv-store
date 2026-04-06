package com.zenz.kvstore.server.restorer;

import com.zenz.kvstore.server.KVStore;

/**
 * Restores the state of a KVMap
 */
public interface BaseRestorer {
    KVStore restore(KVStore.Builder builder) throws Exception;
}
