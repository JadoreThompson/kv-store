package com.zenz.kvstore.server.cli;

import com.zenz.kvstore.server.raft.NodeConfig;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "raft", description = "Run node within a cluster")
public class RaftCli implements Callable<Integer> {
    @Parameters(index = "0", description = "Manager name")
    private String nodeName;

    @Parameters(index = "1", description = "Advertised host")
    private String advertisedHost;

    @Parameters(index = "2", description = "Advertised port")
    private int advertisedPort;

    @Parameters(index = "3", description = "Client host")
    private String clientHost;

    @Parameters(index = "4", description = "Client port")
    private int clientPort;

    @Option(
            names = "-cluster",
            description = "List of known nodes within the cluster. In the format <name>=<nodeHost>:<nodePort>"
    )
    private String cluster;

    @Option(
            names = "-discovery-endpoint",
            description = "Discovery service endpoint if nodes are unknown e.g. discovery.com"
    )
    private String discoveryEndpoint;

    @Override
    public Integer call() throws Exception {
        return 1;
    }

    private List<NodeConfig> parseCluster(String cluster) {
        List<NodeConfig> nodes = new ArrayList<>();
        return nodes;
    }
}
