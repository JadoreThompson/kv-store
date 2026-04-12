package com.zenz.kvstore.server.restore;

import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.logging.Logger;
import com.zenz.kvstore.server.logging.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;


public class DummyLogger implements Logger {

    @Override
    public void log(LogEntry logEntry) throws IOException {

    }

    @Override
    public Path getPath() {
        return null;
    }

    @Override
    public LoggerFactory getLoggerFactory() {
        return null;
    }
}
