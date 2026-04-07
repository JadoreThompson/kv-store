package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.client.KVStoreClient;
import com.zenz.kvstore.common.response.GetResponse;
import com.zenz.kvstore.common.response.SearchResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

/**
 * Wrapper for a client which retrieves the node configurations for known members within a cluster.
 * Connects to an internal cluster which is used to manage the known members within a cluster.
 */
public class DiscoveryService {

    private static final String CLUSTER_PREFIX = "cluster/%s/";
    private final InetSocketAddress address;
    private KVStoreClient client;
    private final UUID clusterToken;

    /**
     * @param clusterToken Unique token for the cluster
     * @param address      Address for the {@link com.zenz.kvstore.server.KVServer}
     */
    public DiscoveryService(final UUID clusterToken, final InetSocketAddress address) {
        this.clusterToken = clusterToken;
        this.address = address;
    }

    /**
     * @param clusterToken Unique token for the cluster
     * @return Prefix URl for a cluster
     */
    public static String getClusterUrl(final UUID clusterToken) {
        return String.format(CLUSTER_PREFIX, clusterToken);
    }

    /**
     * @param clusterToken Unique token for the cluster
     * @param memberName   Unique member name
     * @return Returns the URL for a cluster member
     */
    public static String getClusterMemberUrl(final UUID clusterToken, final String memberName) {
        return String.format(CLUSTER_PREFIX + "members/%s", clusterToken, memberName);
    }

    public void addMember(final UUID clusterToken, final NodeConfig nodeConfig) throws IOException, InterruptedException {
        final KVStoreClient client = getClient();
        final String memberUrl = getClusterMemberUrl(clusterToken, nodeConfig.name());
        client.put(memberUrl, nodeConfig.serialize());
    }

    public void deleteMember(final UUID clusterToken, final String memberName) throws IOException, InterruptedException {
        final KVStoreClient client = getClient();
        final String memberUrl = getClusterMemberUrl(clusterToken, memberName);
        client.delete(memberUrl);
    }

    public NodeConfig getMember(final UUID clusterToken, final String memberName) throws IOException, InterruptedException {
        final KVStoreClient client = getClient();
        final String memberUrl = getClusterMemberUrl(clusterToken, memberName);
        GetResponse response = client.get(memberUrl);
        return NodeConfig.deserialize(ByteBuffer.wrap(response.value()));
    }

    public List<NodeConfig> getMembers() throws IOException, InterruptedException {
        final KVStoreClient client = getClient();
        final String clusterUrl = getClusterUrl(this.clusterToken);
        final SearchResponse response = client.search(clusterUrl);
        return response.entries()
                .stream()
                .map(entry -> NodeConfig.deserialize(ByteBuffer.wrap(entry.value())))
                .toList();
    }

    public void disconnect() throws IOException {
        if (this.client != null) {
            this.client.disconnect();
        }
    }

    private KVStoreClient getClient() throws IOException {
        if (this.client == null) {
            this.client = new KVStoreClient(this.address.getHostName(), this.address.getPort());
            this.client.connect();
        }
        return this.client;
    }
}
