package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.Message;

/**
 * No-op server observer
 */
public class RaftServerObserver implements ServerObserver {

    public void onSend(final Message message) {
    }

    public void onReceive(final Message message) {
    }
}
