package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.Message;

import java.net.InetSocketAddress;

/**
 * No-op client observer
 */
public class RaftClientObserver implements ClientObserver {

    public void onSend(final InetSocketAddress to, final Message message) {
    }

    public void onReceive(final InetSocketAddress to, final Message message) {
    }
}
