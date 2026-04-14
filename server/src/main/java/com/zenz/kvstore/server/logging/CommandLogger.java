package com.zenz.kvstore.server.logging;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface CommandLogger {

    void log(LogEntry logEntry) throws IOException;

    Path getPath();

    LoggerFactory getLoggerFactory();

    void close() throws IOException;

    <L extends LogEntry> List<L> loadLogs(Path path, Deserializer<L> deserializer) throws IOException;
}