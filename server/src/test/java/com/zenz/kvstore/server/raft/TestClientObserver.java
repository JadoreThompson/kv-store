package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.raft.message.Message;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestClientObserver implements ClientObserver {

    public final Map<InetSocketAddress, List<Message>> sentMessages = new HashMap<InetSocketAddress, List<Message>>();
    public final Map<InetSocketAddress, List<Message>> receivedMessages = new HashMap<InetSocketAddress, List<Message>>();

    @Override
    public void onSend(final InetSocketAddress to, final Message message) {
        sentMessages.computeIfAbsent(to, k -> new ArrayList<Message>()).add(message);
    }

    @Override
    public void onReceive(final InetSocketAddress from, final Message message) {
        receivedMessages.computeIfAbsent(from, k -> new ArrayList<Message>()).add(message);
    }
}