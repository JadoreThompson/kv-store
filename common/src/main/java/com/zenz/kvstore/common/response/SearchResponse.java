package com.zenz.kvstore.common.response;

import com.zenz.kvstore.common.enums.ResponseType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public record SearchResponse(ResponseType type, List<Entry> entries) implements BaseResponse {

    @Override
    public byte[] serialize() {
        AtomicInteger entryBytesSize = new AtomicInteger();
        List<byte[]> entryBytes = entries
                .stream()
                .map(entry -> {
                    final byte[] bytes = entry.serialize();
                    entryBytesSize.addAndGet(4 + bytes.length);
                    return bytes;
                }).toList();

        final ByteBuffer buffer = ByteBuffer.allocate(4 + entryBytesSize.get());

        buffer.putInt(type.getValue());
        for (byte[] bytes : entryBytes) {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }

        return buffer.array();
    }

    public static SearchResponse deserialize(ByteBuffer buffer) {
        final ResponseType type = ResponseType.fromValue(buffer.getInt());
        if (type != ResponseType.SEARCH_RESPONSE) {
            throw new IllegalArgumentException("Invalid response type: " + type);
        }

        List<Entry> entries = new ArrayList<>();
        while (buffer.hasRemaining()) {
            final int entrySize = buffer.getInt();
            final byte[] entryBytes = new byte[entrySize];
            buffer.get(entryBytes);
            entries.add(Entry.deserialize(ByteBuffer.wrap(entryBytes)));
        }

        return new SearchResponse(type, entries);
    }

    public record Entry(String key, byte[] value) {

        public byte[] serialize() {
            final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = value.clone();
            final ByteBuffer buffer = ByteBuffer.allocate(
                    4 +
                            keyBytes.length +
                            4 +
                            valueBytes.length);

            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);

            return buffer.array();
        }

        public static Entry deserialize(ByteBuffer buffer) {
            final int keyLength = buffer.getInt();
            final byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);

            final int valueLength = buffer.getInt();
            final byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            return new Entry(new String(keyBytes, StandardCharsets.UTF_8), valueBytes);
        }
    }
}
