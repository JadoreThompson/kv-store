package com.zenz.kvstore.server.raft;

import java.net.InetSocketAddress;

public record RaftNodeConfig(
        long id, // Manager's id
        InetSocketAddress nodeAddress, // The node's broker or controller server host
        InetSocketAddress serverAddress, // The node's client facing KV server host
        NodeRole state // Manager's initial state
) {
}