package com.zenz.kvstore.server.raft;

import java.net.InetSocketAddress;

public record NodeConfig(String name, InetSocketAddress address) {
    @Override
    public String toString() {
        return "NodeConfig{" +
                "name='" + name + '\'' +
                ", address=" + address +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeConfig nodeConfig = (NodeConfig) o;
        if (name != null ? !name.equals(nodeConfig.name) : nodeConfig.name != null) return false;
        return address != null ? address.equals(nodeConfig.address) : nodeConfig.address == null;

    }
}
