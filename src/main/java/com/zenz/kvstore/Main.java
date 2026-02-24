package com.zenz.kvstore;

import java.io.IOException;

public class Main {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6767;

    public static void main(String[] args) throws IOException {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h":
                case "--host":
                    if (i + 1 < args.length) {
                        host = args[++i];
                    } else {
                        System.err.println("Error: " + args[i] + " requires a value");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                case "-p":
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            port = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: Port must be a valid number");
                            printUsage();
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Error: " + args[i] + " requires a value");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                default:
                    System.err.println("Error: Unknown option: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        System.out.println("Starting KV Connection Manager on " + host + ":" + port);
        KVStore store = new KVStore.Builder().build();
        KVConnectionManager connManager = new KVConnectionManager(host, port, store);
        connManager.start();
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar kv-store.jar [options]");
        System.out.println("Options:");
        System.out.println("\t-h, --host <host>  Host address to bind to (default: localhost)");
        System.out.println("\t-p, --port <port>  Port to listen on (default: 6767)");
    }
}
