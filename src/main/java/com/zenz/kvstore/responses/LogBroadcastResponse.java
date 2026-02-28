package com.zenz.kvstore.responses;

import com.zenz.kvstore.ResponseStatus;
import com.zenz.kvstore.ResponseType;
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
public record LogBroadcastResponse(
        ResponseStatus status,
        ResponseType type,
        long logId,
        long term,
        Command command
) implements BaseResponse {

    public LogBroadcastResponse(long logId, long term, Command command) {
        this(ResponseStatus.SUCCESS, ResponseType.LOG, logId, term, command);
    }

    @Override
    public byte[] serialize() {
        byte[] commandBytes = command.serialize();
        ByteBuffer buffer = ByteBuffer.allocate(
                4 + 4 + 8 + 8 + 4 + commandBytes.length);

        buffer.putInt(status.getValue());
        buffer.putInt(type.getValue());
        buffer.putLong(logId);
        buffer.putLong(term);
        buffer.putInt(commandBytes.length);
        buffer.put(commandBytes);

        return buffer.array();
    }

    public static LogBroadcastResponse deserialize(byte[] bytes) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            int statusValue = buffer.getInt();
            int typeValue = buffer.getInt();
            ResponseType type = ResponseType.fromValue(typeValue);
            if (!type.equals(ResponseType.BROADCAST)) {
                throw new IllegalArgumentException("Invalid message type");
            }

            long logId = buffer.getLong();
            long term = buffer.getLong();
            int commandByteLength = buffer.getInt();
            byte[] commandBytes = new byte[commandByteLength];
            buffer.get(commandBytes);
            Command command = Command.deserialize(commandBytes);

            return new LogBroadcastResponse(logId, term, command);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    public static LogBroadcastResponse deserialize(ByteBuffer buffer) {
        try {
            int statusValue = buffer.getInt();
            int typeValue = buffer.getInt();
            ResponseType type = ResponseType.fromValue(typeValue);
            if (!type.equals(ResponseType.BROADCAST)) {
                throw new IllegalArgumentException("Invalid message type");
            }

            long logId = buffer.getLong();
            long term = buffer.getLong();
            int commandByteLength = buffer.getInt();
            byte[] commandBytes = new byte[commandByteLength];
            buffer.get(commandBytes);
            Command command = Command.deserialize(commandBytes);

            return new LogBroadcastResponse(logId, term, command);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
