package com.zenz.kvstore.server.logging;

import java.io.IOException;
import java.nio.file.Path;

public class WALoggerFactory implements LoggerFactory {

    @Override
    public WALogger create(final Path path) throws IOException {
        return new WALogger(path);
    }
}