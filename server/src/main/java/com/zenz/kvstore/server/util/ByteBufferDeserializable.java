package com.zenz.kvstore.server.util;

import java.nio.ByteBuffer;

public interface ByteBufferDeserializable<T> {

    T deserialize(ByteBuffer buffer);
}
