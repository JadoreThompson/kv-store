package com.zenz.kvstore.server.cli;

import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVServer;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.command.handler.RaftCommandHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handler.RaftLogHandler;
import com.zenz.kvstore.server.raft.DiscoveryService;
import com.zenz.kvstore.server.raft.Manager;
import com.zenz.kvstore.server.raft.NodeConfig;
import com.zenz.kvstore.server.restorer.RaftRestorer;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Command(name = "raft", description = "Run node within a cluster")
public class RaftCli implements Callable<Integer> {

    @Option(names = "-name", required = true, description = "Manager name")
    private String name;

    @Option(names = "-server-host", required = true, description = "Advertised host")
    private String serverHost;

    @Option(names = "-server-port", required = true, description = "Advertised port")
    private int serverPort;

    @Option(names = "-client-host", required = true, description = "Client host")
    private String clientHost;

    @Option(names = "-client-port", required = true, description = "Client port")
    private int clientPort;

    @Option(names = "-cluster-token", required = true, description = "Cluster token")
    private UUID clusterToken;

    @Option(names = "--discovery-endpoint", description = "Discovery service address")
    private String discoverEndpoint;

    @Option(names = "--discovery-host")
    private String discoveryHost;

    @Option(names = "--discovery-port")
    private Integer discoveryPort;

    @Option(
            names = "--peer",
            description = "Peer in the format <name>,<host>,<port>. Repeat for multiple peers.",
            converter = PeerConverter.class
    )
    private final Set<NodeConfig> peers = new HashSet<>();

    @Option(names = "--logs-dir", description = "Folder path for the logs folder")
    private String logsDir;

    @Option(names = "--snapshots-dir", description = "Folder path for the snapshots folder")
    private String snapshotsDir;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Object exceptionLock = new Object();
    private Exception backgroundException;

    @Override
    public Integer call() throws Exception {
        final NodeConfig nodeConfig =
                new NodeConfig(
                        this.name,
                        new InetSocketAddress(this.serverHost, this.serverPort),
                        new InetSocketAddress(this.clientHost, this.clientPort));

        final KVStore kvStore = new RaftRestorer()
                .restore(
                        new KVStore.Builder()
                                .setSnapshotter(
                                        new KVMapSnapshotter(
                                                this.snapshotsDir == null || this.snapshotsDir.trim().isEmpty()
                                                        ? Config.SNAPSHOTS_DIR : Path.of(this.snapshotsDir.trim())))
                                .setSnapshotEnabled(true)
                                .setLogHandler(new RaftLogHandler(new WALogger(
                                        this.logsDir == null || this.logsDir.trim().isEmpty()
                                                ? Config.LOGS_DIR : Path.of(this.logsDir.trim()).resolve("0.log")))));

        final DiscoveryService discoveryService = getDiscoveryService();
        try {
            if (discoveryService != null) {
                this.peers.addAll(discoveryService.getMembers());
                discoveryService.addMember(nodeConfig);
            }

            final Manager manager = new Manager(kvStore, nodeConfig, new ArrayList<>(this.peers));
            final RaftCommandHandler commandHandler = new RaftCommandHandler(kvStore, manager);
            final KVServer kvServer = new KVServer(this.clientHost, this.clientPort, commandHandler);

            executorService.submit(wrapStart("manager", manager::start));
            executorService.submit(wrapStart("kvServer", kvServer::start));

            synchronized (exceptionLock) {
                while (backgroundException == null) {
                    exceptionLock.wait();
                }
            }
            throw backgroundException;
        } finally {
            executorService.shutdownNow();

            if (discoveryService != null) {
                discoveryService.deleteMember(this.name);
                discoveryService.disconnect();
            }
        }
    }

    private DiscoveryService getDiscoveryService() {
        if (this.discoveryHost == null && this.discoveryPort == null) {
            return null;
        }

        if (
                (this.discoveryHost == null && this.discoveryPort != null) ||
                        (this.discoveryHost != null && this.discoveryPort == null)) {
            throw new IllegalArgumentException("discoveryHost and discoveryPort must be both null");
        }

        this.discoveryHost = this.discoveryHost.trim();
        if (this.discoveryHost.isEmpty()) {
            throw new IllegalArgumentException("discoveryHost must be set");
        }

        return new DiscoveryService(this.clusterToken, new InetSocketAddress(this.discoveryHost, this.discoveryPort));
    }

    private Runnable wrapStart(String componentName, ThrowingRunnable action) {
        return () -> {
            try {
                action.run();
            } catch (Exception e) {
                synchronized (exceptionLock) {
                    if (backgroundException == null) {
                        backgroundException = new RuntimeException("Failed while running " + componentName, e);
                        exceptionLock.notifyAll();
                    }
                }
            }
        };
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static class PeerConverter implements ITypeConverter<NodeConfig> {

        @Override
        public NodeConfig convert(String value) {
            int hostIdx = -1;
            String host = null;
            int portIdx = -1;
            Integer port = null;

            for (int i = value.length() - 1; i >= 0; i--) {
                char c = value.charAt(i);
                if (c == ',') {
                    if (port == null) {
                        port = Integer.parseInt(value.substring(i + 1));
                        portIdx = i;
                    } else {
                        host = value.substring(i + 1, portIdx).trim();
                        if (host.isEmpty()) {
                            throw new TypeConversionException("Invalid host '" + host + "'");
                        }
                        hostIdx = i;
                        break;
                    }
                }
            }

            if (hostIdx == -1) {
                throw new TypeConversionException(
                        "Invalid peer '" + value + "'. Expected format: <name>,<host>,<port>"
                );
            }

            final String name = value.substring(0, hostIdx).trim();
            if (name.isEmpty()) {
                throw new TypeConversionException("Invalid name '" + name + "'");
            }

            return new NodeConfig(name, new InetSocketAddress(host, port), null);
        }
    }
}
