package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.Message;

import java.net.InetSocketAddress;

public interface ClientObserver {

    void onSend(final InetSocketAddress to, final Message message);

    void onReceive(final InetSocketAddress from, final Message message);
}
