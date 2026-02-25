package com.zenz.kvstore.log_handlers;

import com.zenz.kvstore.BaseLog;
import com.zenz.kvstore.WALogger;
import com.zenz.kvstore.commands.Command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class LogHandler implements BaseLogHandler {
    private static Path DEFAULT_LOG_DIR = Path.of("logs");
    private final Path logDir;
    private WALogger logger;
    private long logId;

    public LogHandler() {
        this(DEFAULT_LOG_DIR);
    }

    public LogHandler(Path logDir) {
        this.logDir = logDir;
    }

    @Override
    public void log(Command command) throws IOException {
        logId++;
        Log log = new Log(logId, command);
        byte[] logBytes = log.serialize();
        ByteBuffer buffer = ByteBuffer.wrap(logBytes);
        logger.log(buffer);
    }

    public ArrayList<Log> deserialize(Path fpath) throws IOException {
        byte[] bytes = Files.readAllBytes(fpath);
        if (bytes == null || bytes.length == 0) return null;

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        ArrayList<Log> logs = new ArrayList<>();
        while (buffer.hasRemaining()) {
            long id = buffer.getLong();
            int commandLength = buffer.getInt();
            byte[] commandBytes = new byte[commandLength];
            buffer.get(commandBytes);
            Command command = Command.deserialize(commandBytes);
            Log logCommand = new Log(id, command);
            logs.add(logCommand);
            buffer.get(); // Skipping new line char
        }

        return logs;
    }

    @Override
    public Path getLogDir() {
        return logDir;
    }

    @Override
    public WALogger getLogger() {
        return logger;
    }

    @Override
    public void setLogger(WALogger logger) {
        this.logger = logger;
    }

    public long getLogId() {
        return logId;
    }

    public void setLogId(long logId) {
        this.logId = logId;
    }

    public record Log(long id, Command command) implements BaseLog {

        public byte[] serialize() {
            byte[] commandBytes = command.serialize();
            ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + commandBytes.length);

            buffer.putLong(id);
            buffer.putInt(commandBytes.length);
            buffer.put(commandBytes);

            return buffer.array();
        }

        static Log deserialize(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            long id = buffer.getLong();
            int commandLength = buffer.getInt();
            byte[] commandBytes = new byte[commandLength];
            buffer.get(commandBytes);
            Command command = Command.deserialize(commandBytes);

            return new Log(id, command);
        }
    }
}
