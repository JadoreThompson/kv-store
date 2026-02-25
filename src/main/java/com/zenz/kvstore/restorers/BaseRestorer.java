package com.zenz.kvstore.restorers;

import com.zenz.kvstore.KVStore;

/**
 * Restores the state of a KVMap
 */
public interface BaseRestorer {
    KVStore restore(KVStore.Builder builder) throws Exception;
}
