package com.zenz.kvstore.server.logging;

import java.io.IOException;
import java.nio.file.Path;

public interface LoggerFactory {

    CommandLogger create(Path path) throws IOException;
}