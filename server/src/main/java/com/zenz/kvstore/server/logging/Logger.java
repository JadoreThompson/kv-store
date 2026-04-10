package com.zenz.kvstore.server.logging;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Logger {

    void log(ByteBuffer buffer) throws IOException;
}
