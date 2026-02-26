package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.commands.Command;

import java.nio.ByteBuffer;

/**
 * Sent by the controller, this object contains the next log or a snapshot
 * to be restored by a broker who seems to have fallen too far back.
 *
 * @param type
 * @param logId
 * @param term
 * @param dataType
 * @param command
 * @param snapshot
 */
public record LogResponse(MessageType type, long logId, long term, DataType dataType, Command command,
                          byte[] snapshot) implements Message {
    public LogResponse(long logId, long term, DataType dataType, Command command, byte[] snapshot) {
        this(MessageType.LOG_RESPONSE, logId, term, dataType, command, snapshot);
    }

    @Override
    public byte[] serialize() {
        byte[] dataBytes = command == null ? snapshot : command.serialize();
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 8 + 4 + 4 + dataBytes.length);

        buffer.putInt(type.getValue());
        buffer.putLong(logId);
        buffer.putLong(term);
        buffer.putInt(dataType.getValue());
        buffer.putInt(dataBytes.length);
        buffer.put(dataBytes);

        return buffer.array();
    }

    public static LogResponse deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int type = buffer.getInt();
        MessageType messageType = MessageType.fromValue(type);
        if (!messageType.equals(MessageType.LOG_RESPONSE)) {
            throw new IllegalArgumentException("Invalid message type");
        }

        long logId = buffer.getLong();
        long term = buffer.getLong();
        int dataTypeValue = buffer.getInt();
        DataType dataType = DataType.fromValue(dataTypeValue);
        int dataLength = buffer.getInt();
        byte[] dataBytes = new byte[dataLength];
        buffer.get(dataBytes);
        Command command = dataType.equals(DataType.COMMAND) ? Command.deserialize(dataBytes) : null;

        return new LogResponse(logId, term, dataType, command, command == null ? dataBytes : null);
    }

    public enum DataType {
        COMMAND(1),
        SNAPSHOT(2);

        private final int value;

        DataType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static DataType fromValue(int value) {
            for (DataType type : DataType.values()) {
                if (type.getValue() == value) return type;
            }

            throw new IllegalArgumentException("Invalid data type");
        }

        @Override
        public String toString() {
            return name();
        }
    }
}

