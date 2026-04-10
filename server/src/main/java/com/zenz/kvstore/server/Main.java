package com.zenz.kvstore.server;

import com.zenz.kvstore.server.cli.RaftCli;
import com.zenz.kvstore.server.cli.SingleCli;
import picocli.CommandLine;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Args required");
            System.exit(1);
        }

        final String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
        int exitCode;
        switch (args[0]) {
            case "run":
                exitCode = new CommandLine(new SingleCli()).execute(commandArgs);
                break;
            case "raft":
                exitCode = new CommandLine(new RaftCli()).execute(commandArgs);
                break;
            default:
                exitCode = 1;
        }

        System.exit(exitCode);
    }
}