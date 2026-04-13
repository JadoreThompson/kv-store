package com.zenz.kvstore.server.snapshot;

public record Snapshot<
        H extends SnapshotHeader,
        B extends SnapshotBody,
        F extends SnapshotFooter
        >(H header, B body, F footer) {
}