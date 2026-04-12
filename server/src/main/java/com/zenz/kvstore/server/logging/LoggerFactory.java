package com.zenz.kvstore.server.logging;

import java.io.IOException;
import java.nio.file.Path;

public interface LoggerFactory {

    Logger create(Path path) throws IOException;
}