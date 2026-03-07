package com.zenz.kvstore.server.raft;

import java.net.InetSocketAddress;

public record RaftNode(
        long id, // Node's id
        InetSocketAddress nodeAddress, // The node's broker or controller server host
        InetSocketAddress serverAddress, // The node's client facing KV server host
        NodeState state // Node's initial state
) {
}