package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.util.ByteArraySerializable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public abstract class SnapshotHeader implements ByteArraySerializable {

    protected final long version;
    protected final long firstLogId;
    protected final long lastLogId;
}
