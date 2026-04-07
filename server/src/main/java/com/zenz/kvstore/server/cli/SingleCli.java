package com.zenz.kvstore.server.cli;


import com.zenz.kvstore.server.Config;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVServer;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.command.handler.CommandHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.handler.LogHandler;
import com.zenz.kvstore.server.restorer.Restorer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "single", description = "Running an isolated node")
public class SingleCli implements Callable<Integer> {
    @Parameters(index = "0", description = "Host")
    private String host;

    @Parameters(index = "1", description = "Port")
    private int port;

    @Override
    public Integer call() throws Exception {
        KVStore.Builder builder = new KVStore.Builder()
                .setSnapshotter(new KVMapSnapshotter())
                .setSnapshotEnabled(true)
                .setLogHandler(new LogHandler(new WALogger(Config.LOGS_DIR.resolve("0.log"))));
        KVStore store = new Restorer().restore(builder);

        CommandHandler commandHandler = new CommandHandler(store);
        KVServer server = new KVServer(host, port, commandHandler);

        server.start();
        return 0;
    }
}
