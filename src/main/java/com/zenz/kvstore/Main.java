package com.zenz.kvstore;

import javax.print.DocFlavor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Main {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6767;

    public static void main(String[] args) throws IOException {
        Path fpath = Path.of("test");
        if (!Files.exists(fpath)) fpath.toFile().createNewFile();
        FileChannel channel = FileChannel.open(fpath, StandardOpenOption.APPEND);
        ByteBuffer src = ByteBuffer.wrap("Hello".getBytes());
        channel.write(src);
        channel.write(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));
        src = ByteBuffer.wrap("Hello".getBytes());
        channel.write(src);
        channel.force(true);

        byte[] bytes = Files.readAllBytes(fpath);
        for (byte b : bytes) {
            System.out.println((char) b);
        }

//        byte[] arr = Files.readAllBytes(fpath);
//        ByteBuffer bb = ByteBuffer.wrap(arr);
//
//        StringBuilder builder = new StringBuilder();
//        bb.position(0);
//        bb.limit(bb.capacity());
//
//        for (byte b : bb.array()) {
//            System.out.print((char) b == 'l');
//            builder.append((char) b);
//        }
//
//        System.out.println(builder.toString());


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
        System.out.println("\t--p, --port <port>  Port to listen on (default: 6767)");
    }
}
