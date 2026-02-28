package com.zenz.kvstore;

import com.zenz.kvstore.commandHandlers.CommandHandler;
import com.zenz.kvstore.restorers.Restorer;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6767;

    public static void main(String[] args) throws Exception {
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

        System.out.println("Starting KV Server on " + host + ":" + port);
        Restorer restorer = new Restorer();
        KVStore.Builder builder = new KVStore.Builder();
        KVStore store = restorer.restore(builder);
        CommandHandler commandHandler = new CommandHandler(store);
        KVServer server = new KVServer(host, port, commandHandler);
        server.start();
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar kv-store.jar [options]");
        System.out.println("Options:");
        System.out.println("\t-h, --host <host>  Host address to bind to (default: localhost)");
        System.out.println("\t--p, --port <port>  Port to listen on (default: 6767)");
    }
}
