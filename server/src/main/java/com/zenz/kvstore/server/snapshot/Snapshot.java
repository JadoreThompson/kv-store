package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.util.ByteArraySerializable;
import com.zenz.kvstore.server.util.ByteBufferDeserializable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

public class Snapshot {

    @Getter
    @RequiredArgsConstructor
    public static abstract class Header implements ByteArraySerializable {

        protected final long version;
        protected final long firstLogId;
        protected final long lastLogId;
    }

    public interface HeaderCreator<L extends LogEntry> extends Creator<Header, L> {
    }

    public interface HeaderDeserializer extends ByteBufferDeserializable<Header> {
    }

    public interface Body extends ByteArraySerializable {

        List<? extends LogEntry> getEntries();
    }

    public interface BodyCreator<L extends LogEntry> extends Creator<Body, L> {
    }

    public interface BodyDeserializer extends ByteBufferDeserializable<Body> {
    }


    @Getter
    @RequiredArgsConstructor
    public static abstract class Footer implements ByteArraySerializable {

        protected final long timestamp;
    }

    public interface FooterCreator<L extends LogEntry> extends Creator<Footer, L> {
    }

    public interface FooterDeserializer extends ByteBufferDeserializable<Footer> {
    }
}
