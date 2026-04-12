package com.zenz.kvstore.server.logging;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LogEntryRegister {

    private static final Map<Class<? extends LogEntry>, Deserializer<?>> deserializers = new HashMap<>();

    public static void register(final Class<? extends LogEntry> clazz, final Deserializer<?> deserializer) {
        deserializers.put(clazz, deserializer);
    }

    public static <L extends LogEntry> L deserialize(
            final Class<L> clazz, final ByteBuffer buffer) throws NotFoundException {
        final Deserializer<?> deserializer = deserializers.get(clazz);
        if (deserializer == null) {
            throw new NotFoundException("Deserializer for " + clazz.getName() + " not registered");
        }
        return (L) deserializer.deserialize(buffer);
    }

    public static class NotFoundException extends RuntimeException {

        public NotFoundException(final String message) {
            super(message);
        }
    }
}
