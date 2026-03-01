package com.zenz.kvstore.raft.messages;


import com.zenz.kvstore.KVMap;
import com.zenz.kvstore.MessageType;
import com.zenz.kvstore.raft.NodeState;
import com.zenz.kvstore.raft.RaftManager;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public record SwitchMessage(
        MessageType type,
        long nodeId,
        NodeState prevState,
        NodeState state
) implements BaseMessage {

    public SwitchMessage(long nodeId, NodeState prevState, NodeState state) {
        this(MessageType.SWITCH, nodeId, prevState, state);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 4 + 4);

        buffer.putInt(type.getValue());
        buffer.putLong(nodeId);
        buffer.putInt(prevState.getValue());
        buffer.putInt(state.getValue());

        return buffer.array();
    }

    public static SwitchMessage deserialize(ByteBuffer buffer) {
        try {
            int typeValue = buffer.getInt();
            MessageType type = MessageType.fromValue(typeValue);
            if (!type.equals(MessageType.SWITCH)) {
                throw new IllegalArgumentException("Invalid message type");
            }

            long nodeId = buffer.getLong();
            NodeState prevState = NodeState.fromValue(buffer.getInt());
            NodeState state = NodeState.fromValue(buffer.getInt());

            return new SwitchMessage(nodeId, prevState, state);
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
