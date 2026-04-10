package com.zenz.kvstore.server.logging;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Snapshotter<L extends LogEntry> {

    Path snapshot(List<L> entries) throws IOException;
}
