package com.zenz.kvstore.server.logging;

import java.io.IOException;

public interface Logger {

    void log(LogEntry logEntry) throws IOException;
}
