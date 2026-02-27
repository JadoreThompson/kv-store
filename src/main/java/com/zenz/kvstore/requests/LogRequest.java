package com.zenz.kvstore.requests;

import com.zenz.kvstore.RequestType;
import com.zenz.kvstore.commands.Command;

import java.nio.ByteBuffer;

/**
 * Sent by a broker, this object contains the last log seen
 * by a broker. The controller will evaluate this log and return a
 * corresponding LogResponse to be evaluated by the broker.
 *
 * @param type
 * @param logId
 * @param term
 * @param command
 */
public record LogRequest(RequestType type, long logId, long term, Command command) implements BaseRequest {
    public LogRequest(long logId, long term, Command command) {
        this(RequestType.LOG, logId, term, command);
    }

    @Override
    public byte[] serialize() {
        byte[] commandBytes = command == null ? new byte[0] : command.serialize();
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + commandBytes.length);

        buffer.putInt(type.getValue());
        buffer.putLong(logId);
        buffer.putLong(term);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }

    public static LogRequest deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int typeValue = buffer.getInt();
        RequestType type = RequestType.fromValue(typeValue);
        if (!type.equals(RequestType.LOG)) {
            throw new IllegalArgumentException("Invalid message type");
        }

        long logId = buffer.getLong();
        long term = buffer.getLong();
        int commandByteLength = buffer.getInt();
        byte[] commandBytes = new byte[commandByteLength];
        buffer.get(commandBytes);
        Command command = Command.deserialize(commandBytes);

        return new LogRequest(logId, term, command);
    }
}
