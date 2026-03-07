package com.zenz.kvstore.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.zenz.kvstore.server.commandHandlers.CommandHandler;
import com.zenz.kvstore.server.logHandlers.LogHandler;
import com.zenz.kvstore.server.logHandlers.RaftLogHandler;
import com.zenz.kvstore.server.raft.RaftManager;
import com.zenz.kvstore.server.raft.RaftNode;
import com.zenz.kvstore.server.restorers.RaftRestorer;
import com.zenz.kvstore.server.restorers.Restorer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6767;
    private static final Path logsDir = Path.of("logs");
    private static final Path snapshotsDir = Path.of("snapshots");

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        switch (args[0]) {

            case "run":
                runSingleNode(args);
                break;

            case "raft":
                runRaft(args);
                break;

            default:
                System.err.println("Unknown command: " + args[0]);
                printUsage();
                System.exit(1);
        }
    }

    private static void runSingleNode(String[] args) throws Exception {

        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;

        for (int i = 1; i < args.length; i++) {

            switch (args[i]) {

                case "-h":
                case "--host":
                    if (i + 1 >= args.length) fail(args[i] + " requires a value");
                    host = args[++i];
                    break;

                case "-p":
                case "--port":
                    if (i + 1 >= args.length) fail(args[i] + " requires a value");

                    try {
                        port = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        fail("Port must be a number");
                    }
                    break;

                default:
                    fail("Unknown option: " + args[i]);
            }
        }

        System.out.println("Starting KV Server (single node) on " + host + ":" + port);

        KVStore.Builder builder = new KVStore.Builder()
                .setSnapshotter(new KVMapSnapshotter(snapshotsDir))
                .setSnapshotEnabled(true)
                .setLogHandler(new LogHandler(new WALogger(logsDir.resolve("0.log"))));
        KVStore store = new Restorer().restore(builder);

        CommandHandler commandHandler = new CommandHandler(store);
        KVServer server = new KVServer(host, port, commandHandler);

        server.start();
    }

    private static void runRaft(String[] args) throws Exception {

        Path configFile = null;
        long nodeId = -1;

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-f":
                    if (i + 1 >= args.length) fail("-f requires a file path");
                    configFile = Path.of(args[++i]);
                    break;
                case "--id":
                    if (i + 1 >= args.length) fail("--id requires a value");
                    nodeId = Long.parseLong(args[++i]);
                    break;
                default:
                    fail("Unknown option: " + args[i]);
            }
        }

        if (configFile == null) {
            fail("Raft mode requires -f <config-file>");
        }

        if (nodeId == -1) {
            fail("Raft mode requires -id <node-id>");
        }

        ObjectMapper mapper = new ObjectMapper();
        List<RaftNode> nodes = mapper.readValue(
                configFile.toFile(),
                new TypeReference<ArrayList<RaftNode>>() {
                }
        );

        boolean found = false;
        for (RaftNode node : nodes) {
            if (node.id() == nodeId) {
                found = true;
                break;
            }
        }

        if (!found) {
            fail("Failed to find config for node with id " + nodeId + " does not exist");
        }

        ensureLogsDir();
        ensureSnapshotsDir();

        KVStore store = new RaftRestorer().restore(new KVStore.Builder()
                .setSnapshotter(new KVMapSnapshotter(snapshotsDir))
                .setSnapshotEnabled(true)
                .setLogHandler(new RaftLogHandler(new WALogger(logsDir.resolve("0.log"))))
        );

        RaftManager manager = new RaftManager(nodeId, new ArrayList<>(nodes), store);
        manager.start();
        manager.join();
    }

    private static void fail(String message) {
        System.err.println("Error: " + message);
        printUsage();
        System.exit(1);
    }

    private static void printUsage() {

        System.out.println("Usage:");
        System.out.println("  run [options]");
        System.out.println("  raft -f <config-file> --id <node-id>");
        System.out.println();

        System.out.println("Single node options:");
        System.out.println("  -h, --host <host>   Host (default: localhost)");
        System.out.println("  -p, --port <port>   Port (default: 6767)");

        System.out.println();

        System.out.println("Raft options:");
        System.out.println("  -f <file>           Path to raft JSON config");
    }

    private static void ensureLogsDir() throws IOException {
        if (!Files.exists(logsDir)) Files.createDirectories(logsDir);
    }

    private static void ensureSnapshotsDir() throws IOException {
        if (!Files.exists(snapshotsDir)) Files.createDirectories(snapshotsDir);
    }
}