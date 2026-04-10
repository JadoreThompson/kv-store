package com.zenz.kvstore.server;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.logging.handler.BaseLogHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class KVStore {

    private final KVMap map;
    private final Trie trie = new Trie('\0');
    private final BaseLogHandler logHandler;

    public KVStore(final KVMap map, final BaseLogHandler logHandler) {
        this.map = map;
        this.logHandler = logHandler;
    }

    public void put(String key, byte[] value) throws IOException {
        this.logHandler.log(new PutCommand(key, value));
        this.map.put(key, value);
        this.trie.add(key);
    }

    public KVMap.Node get(String key) {
        return this.map.get(key);
    }

    public boolean delete(String key) throws IOException {
        this.logHandler.log(new DeleteCommand(key));
        if (this.map.remove(key)) {
            this.trie.remove(key);
            return true;
        }
        return false;
    }

    /**
     * Returns all nodes whose keys share the prefix
     *
     * @param prefix
     * @return
     */
    public List<KVMap.Node> search(final String prefix) {
        final List<String> keys = this.trie.search(prefix);
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        return keys.stream().map(this::get).toList();
    }

    public KVMap getMap() {
        return this.map;
    }
}