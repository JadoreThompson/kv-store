package com.zenz.kvstore.server.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Config {

    public static final Path LOGS_DIR = Path.of("logs");
    public static final Path SNAPSHOTS_DIR = Path.of("snapshots");

    static {
        try {
            Files.createDirectories(LOGS_DIR);
            Files.createDirectories(SNAPSHOTS_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
