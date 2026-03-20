package com.zenz.kvstore.server.raft.server.handlers;

import com.zenz.kvstore.server.ClientSession;

import java.nio.channels.SocketChannel;

class RaftClientSession extends ClientSession {
    private long logId = -1;
    private long term = -1;

    public RaftClientSession(SocketChannel channel) {
        super(channel);
    }

    public long getLogId() {
        return logId;
    }

    public void setLogId(long logId) {
        this.logId = logId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }
}
