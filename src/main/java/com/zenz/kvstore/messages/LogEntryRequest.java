package com.zenz.kvstore.messages;

import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.operations.Operation;

import java.nio.ByteBuffer;

public class LogEntryRequest extends Message {
    private final int term;
    private final int index;
    private final Operation operation;
    private final boolean commited;

    public LogEntryRequest(int term, int index, Operation operation, boolean commited) {
        this.term = term;
        this.index = index;
        this.operation = operation;
        this.commited = commited;
    }

    @Override
    public MessageType type() {
        return MessageType.LOG_REQUEST;
    }

    @Override
    public byte[] serialize() {
        byte[] operationBytes = operation.serialize();
        // type(4) + term(4) + index(4) + operationLength(4) + operation + committed(1)
        int totalSize = 4 + 4 + 4 + 4 + operationBytes.length + 1;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(type().getValue());
        buffer.putInt(term);
        buffer.putInt(index);
        buffer.putInt(operationBytes.length);
        buffer.put(operationBytes);
        buffer.put((byte) (commited ? 1 : 0));
        return buffer.array();
    }

    public static LogEntryRequest deserialize(ByteBuffer buffer) {
        // Type is already read by the caller
        int term = buffer.getInt();
        int index = buffer.getInt();
        int operationLength = buffer.getInt();
        byte[] operationBytes = new byte[operationLength];
        buffer.get(operationBytes);
        Operation operation = Operation.deserialize(ByteBuffer.wrap(operationBytes));
        boolean committed = buffer.get() == 1;
        return new LogEntryRequest(term, index, operation, committed);
    }

    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public Operation getOperation() {
        return operation;
    }

    public boolean isCommited() {
        return commited;
    }

    public static LogEntryRequest fromString(String[] components) {
        if (components.length != 4) throw new IllegalArgumentException("LogEntryRequest must have 4 components");

        int term = Integer.parseInt(components[0]);
        int index = Integer.parseInt(components[1]);
        Operation operation = Operation.fromLine(components[2]);
        boolean committed = Boolean.parseBoolean(components[3]);
        return new LogEntryRequest(term, index, operation, committed);
    }

    @Override
    public String toString() {
        return type().toString() + " " + term + " " + index + " " + operation.toString() + " " + commited;
    }
}