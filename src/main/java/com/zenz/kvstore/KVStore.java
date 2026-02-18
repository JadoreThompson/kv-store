package main.java.com.zenz.kvstore;

public class KVStore {
    private final Map map;

    public KVStore() {
        map = new Map();
    }

    public Map.Node get(String key) {
        return map.get(key);
    }

    public void put(String key, byte[] value) {
        map.put(key, value);
    }
}
