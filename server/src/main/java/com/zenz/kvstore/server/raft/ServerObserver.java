package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.Message;

public interface ServerObserver {

    void onSend(final Message message);

    void onReceive(final Message message);
}
