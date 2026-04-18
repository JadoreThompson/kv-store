package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.Message;

import java.util.ArrayList;
import java.util.List;

class TestServerObserver implements ServerObserver {

    public final List<Message> sentMessages = new ArrayList<>();
    public final List<Message> receivedMessages = new ArrayList<>();

    @Override
    public void onSend(final Message message) {
        sentMessages.add(message);
    }

    @Override
    public void onReceive(final Message message) {
        receivedMessages.add(message);
    }
}
