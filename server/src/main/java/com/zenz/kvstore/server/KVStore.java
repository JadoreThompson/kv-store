package com.zenz.kvstore.server;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.logging.BaseLogHandler;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class KVStore {

    @Getter
    private final KVMap map;

    @Getter
    private final Trie trie = new Trie('\0');

    @Getter
    @Setter
    private BaseLogHandler<?, ?> logHandler;

    public KVStore(final BaseLogHandler<?, ?> logHandler) {
        this.logHandler = logHandler;
        map = new KVMap();
    }

    public KVStore(final KVMap map, final BaseLogHandler<?, ?> logHandler) {
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
        final List<String> keys = this.trie.get(prefix);
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        return keys.stream().map(this::get).toList();
    }
}