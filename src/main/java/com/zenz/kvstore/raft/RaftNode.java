package com.zenz.kvstore.raft;

import java.net.InetSocketAddress;

public record RaftNode(long id, InetSocketAddress address, NodeState state) {
}