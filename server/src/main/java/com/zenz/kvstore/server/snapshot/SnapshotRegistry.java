package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotRegistry {

    private static final Map<Class<? extends Snapshot.Header>, Snapshot.HeaderDeserializer> headerDeserializerRegistry
            = new HashMap<>();
    private static final Map<Class<? extends Snapshot.Body>, Snapshot.BodyDeserializer> bodyDeserializerRegistry
            = new HashMap<>();
    private static final Map<Class<? extends Snapshot.Footer>, Snapshot.FooterDeserializer> footerDeserializerRegistry
            = new HashMap<>();

    private static final Map<Class<? extends Snapshot.Header>, Snapshot.HeaderCreator<?>> headerCreatorRegistry
            = new HashMap<>();
    private static final Map<Class<? extends Snapshot.Body>, Snapshot.BodyCreator<?>> bodyCreatorRegistry
            = new HashMap<>();
    private static final Map<Class<? extends Snapshot.Footer>, Snapshot.FooterCreator<?>> footerCreatorRegistry
            = new HashMap<>();

    public static void registerHeaderDeserializer(
            final Class<? extends Snapshot.Header> clazz,
            final Snapshot.HeaderDeserializer deserializer) {
        headerDeserializerRegistry.put(clazz, deserializer);
    }

    public static <T extends Snapshot.Header> T deserializeHeader(
            final Class<T> clazz, final ByteBuffer buffer) {

        final Snapshot.HeaderDeserializer deserializer = headerDeserializerRegistry.get(clazz);

        if (deserializer == null) {
            throw new NotFoundException("Header deserializer for " + clazz.getName() + " not registered");
        }

        return clazz.cast(deserializer.deserialize(buffer));
    }

    public static void registerBodyDeserializer(
            final Class<? extends Snapshot.Body> clazz,
            final Snapshot.BodyDeserializer deserializer) {
        bodyDeserializerRegistry.put(clazz, deserializer);
    }

    public static <T extends Snapshot.Body> T deserializeBody(
            final Class<T> clazz, final ByteBuffer buffer) {

        final Snapshot.BodyDeserializer deserializer = bodyDeserializerRegistry.get(clazz);

        if (deserializer == null) {
            throw new NotFoundException("Body deserializer for " + clazz.getName() + " not registered");
        }

        return clazz.cast(deserializer.deserialize(buffer));
    }

    public static void registerFooterDeserializer(
            final Class<? extends Snapshot.Footer> clazz,
            final Snapshot.FooterDeserializer deserializer) {
        footerDeserializerRegistry.put(clazz, deserializer);
    }

    public static <T extends Snapshot.Footer> T deserializeFooter(
            final Class<T> clazz, final ByteBuffer buffer) {

        final Snapshot.FooterDeserializer deserializer = footerDeserializerRegistry.get(clazz);

        if (deserializer == null) {
            throw new NotFoundException("Footer deserializer for " + clazz.getName() + " not registered");
        }

        return clazz.cast(deserializer.deserialize(buffer));
    }

    public static void registerHeaderCreator(
            final Class<? extends Snapshot.Header> clazz,
            final Snapshot.HeaderCreator<?> creator) {
        headerCreatorRegistry.put(clazz, creator);
    }

    public static <T extends Snapshot.Header, L extends LogEntry> T createHeader(
            final Class<T> clazz, final List<L> entries) {

        final Snapshot.HeaderCreator<L> creator =
                (Snapshot.HeaderCreator<L>) headerCreatorRegistry.get(clazz);

        if (creator == null) {
            throw new NotFoundException("Header creator for " + clazz.getName() + " not registered");
        }

        return clazz.cast(creator.create(entries));
    }

    public static void registerBodyCreator(
            final Class<? extends Snapshot.Body> clazz,
            final Snapshot.BodyCreator<?> creator) {
        bodyCreatorRegistry.put(clazz, creator);
    }

    public static <T extends Snapshot.Body, L extends LogEntry> T createBody(
            final Class<T> clazz, final List<L> entries) {

        final Snapshot.BodyCreator<L> creator =
                (Snapshot.BodyCreator<L>) bodyCreatorRegistry.get(clazz);

        if (creator == null) {
            throw new NotFoundException("Body creator for " + clazz.getName() + " not registered");
        }

        return clazz.cast(creator.create(entries));
    }

    public static void registerFooterCreator(
            final Class<? extends Snapshot.Footer> clazz,
            final Snapshot.FooterCreator<?> creator) {
        footerCreatorRegistry.put(clazz, creator);
    }

    public static <T extends Snapshot.Footer, L extends LogEntry> T createFooter(
            final Class<T> clazz, final List<L> entries) {

        final Snapshot.FooterCreator<L> creator =
                (Snapshot.FooterCreator<L>) footerCreatorRegistry.get(clazz);

        if (creator == null) {
            throw new NotFoundException("Footer creator for " + clazz.getName() + " not registered");
        }

        return clazz.cast(creator.create(entries));
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(final String message) {
            super(message);
        }
    }
}