package com.zenz.kvstore.server.raft;

import java.net.InetSocketAddress;

public record NodeConfig(String id, InetSocketAddress address) {
}
