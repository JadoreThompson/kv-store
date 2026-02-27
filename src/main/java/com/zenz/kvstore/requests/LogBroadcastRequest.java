package com.zenz.kvstore.requests;

import com.zenz.kvstore.ResponseStatus;
import com.zenz.kvstore.RequestType;
import com.zenz.kvstore.commands.Command;

import java.nio.ByteBuffer;

/**
 * Sent by the controller, this object contains the most recent log
 * seen by the controller.
 *
 * @param type
 * @param logId
 * @param term
 * @param command
 */
public record LogBroadcastRequest(ResponseStatus status, RequestType type, long logId, long term,
                                  Command command) implements BaseRequest {
    public LogBroadcastRequest(long logId, long term, Command command) {
        this(ResponseStatus.SUCCESS, RequestType.LOG, logId, term, command);
    }

    @Override
    public byte[] serialize() {
        byte[] commandBytes = command.serialize();
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + commandBytes.length);

        buffer.putInt(type.getValue());
        buffer.putLong(logId);
        buffer.putLong(term);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }

    public static LogBroadcastRequest deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int typeValue = buffer.getInt();
        RequestType type = RequestType.fromValue(typeValue);
        if (!type.equals(RequestType.BROADCAST)) {
            throw new IllegalArgumentException("Invalid message type");
        }

        long logId = buffer.getLong();
        long term = buffer.getLong();
        int commandByteLength = buffer.getInt();
        byte[] commandBytes = new byte[commandByteLength];
        buffer.get(commandBytes);
        Command command = Command.deserialize(commandBytes);

        return new LogBroadcastRequest(logId, term, command);
    }
}
