package com.zenz.kvstore;

public class KVStore {
    private final KVMap map;

    public KVStore() {
        map = new KVMap();
    }

    public KVMap.Node get(String key) {
        return map.get(key);
    }

    public void put(String key, byte[] value) {
        map.put(key, value);
    }
}
