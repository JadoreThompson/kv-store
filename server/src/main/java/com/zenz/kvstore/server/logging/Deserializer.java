package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.server.util.ByteBufferDeserializable;

import java.nio.ByteBuffer;

public interface Deserializer<L extends LogEntry> extends ByteBufferDeserializable<L> {

    L deserialize(ByteBuffer buffer);
}
