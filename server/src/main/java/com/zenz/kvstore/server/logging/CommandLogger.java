package com.zenz.kvstore.server.logging;

import java.io.IOException;
import java.nio.file.Path;

public interface CommandLogger {

    void log(LogEntry logEntry) throws IOException;

    Path getPath();

    LoggerFactory getLoggerFactory();

    void close() throws IOException;
}