package com.zenz.kvstore.server.raft.message;

import com.zenz.kvstore.server.raft.MessageType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record InstallSnapshotV2(
        MessageType type,
        long term,
        long lastLogId,
        long lastLogTerm,
        long offset,
        byte[] data,
        boolean done
) implements Message {

    public InstallSnapshotV2(
            long term,
            long lastLogId,
            long lastLogTerm,
            long offset,
            byte[] data,
            boolean done
    ) {
        this(
                MessageType.INSTALL_SNAPSHOT_V2,
                term,
                lastLogId,
                lastLogTerm,
                offset,
                data,
                done
        );
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(
                4 +     // type
                        8 +     // term
                        8 +     // lastLogId
                        8 +     // lastLogTerm
                        8 +     // offset
                        4 +     // data length
                        data.length +
                        1       // done
        );

        buffer.putInt(type.getValue());
        buffer.putLong(term);
        buffer.putLong(lastLogId);
        buffer.putLong(lastLogTerm);
        buffer.putLong(offset);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.put((byte) (done ? 1 : 0));

        return buffer.array();
    }

    public static InstallSnapshotV2 deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType messageType = MessageType.fromValue(typeValue);
            if (messageType != MessageType.INSTALL_SNAPSHOT) {
                throw new IllegalArgumentException("Invalid message type " + messageType);
            }

            long term = buffer.getLong();
            long lastLogId = buffer.getLong();
            long lastLogTerm = buffer.getLong();
            long offset = buffer.getLong();

            int dataLength = buffer.getInt();
            byte[] data = new byte[dataLength];
            buffer.get(data);

            boolean done = buffer.get() == 1;

            return new InstallSnapshotV2(
                    messageType,
                    term,
                    lastLogId,
                    lastLogTerm,
                    offset,
                    data,
                    done
            );
        } catch (BufferUnderflowException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "InstallSnapshotV2{" +
                "type=" + type +
                ", term=" + term +
                ", lastLogId=" + lastLogId +
                ", lastLogTerm=" + lastLogTerm +
                ", offset=" + offset +
                ", data=" + (data != null ? data.length + " bytes" : "null") +
                ", done=" + done +
                '}';
    }
}