package com.zenz.kvstore.requests;

import com.zenz.kvstore.RequestType;
import com.zenz.kvstore.commands.Command;

import java.nio.BufferUnderflowException;
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
public record LogBroadcastRequest(
        RequestType type,
        long logId,
        long term,
        Command command
) implements BaseRequest {

    public LogBroadcastRequest(long logId, long term, Command command) {
        this(RequestType.LOG, logId, term, command);
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
        try {
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
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    public static LogBroadcastRequest deserialize(ByteBuffer buffer) {
        try {
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
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
