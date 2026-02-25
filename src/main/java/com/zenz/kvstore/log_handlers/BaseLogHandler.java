package com.zenz.kvstore.log_handlers;

import com.zenz.kvstore.WALogger;
import com.zenz.kvstore.commands.Command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

public interface BaseLogHandler {
    void log(Command command) throws IOException;

    ArrayList<LogHandler.Log> deserialize(Path fpath) throws IOException;

    Path getLogDir();

    WALogger getLogger();

    void setLogger(WALogger logger);

    public long getLogId();

    public void setLogId(long logId);
}
