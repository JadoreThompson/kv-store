package com.zenz.kvstore.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Config {
    public static final Path LOGS_DIR = Path.of("logs");

    static {
        try {
            Files.createDirectories(LOGS_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
