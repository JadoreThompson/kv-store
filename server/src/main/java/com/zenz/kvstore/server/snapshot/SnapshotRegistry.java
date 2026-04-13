package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.server.logging.LogEntry;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotRegistry {

    private static final Map<Class<? extends SnapshotHeader>, SnapshotHeaderDeserializer> headerDeserializerRegistry
            = new HashMap<>();
    private static final Map<Class<? extends SnapshotBody>, SnapshotBodyDeserializer> bodyDeserializerRegistry
            = new HashMap<>();
    private static final Map<Class<? extends SnapshotFooter>, SnapshotFooterDeserializer> footerDeserializerRegistry
            = new HashMap<>();

    private static final Map<Class<? extends SnapshotHeader>, SnapshotHeaderCreator<?>> headerCreatorRegistry
            = new HashMap<>();
    private static final Map<Class<? extends SnapshotBody>, SnapshotBodyCreator<?>> bodyCreatorRegistry
            = new HashMap<>();
    private static final Map<Class<? extends SnapshotFooter>, SnapshotFooterCreator<?>> footerCreatorRegistry
            = new HashMap<>();

    static {
        registerHeaderCreator(SingleSnapshotHeader.class, new SingleSnapshotHeaderCreator());
        registerHeaderCreator(RaftSnapshotHeader.class, new RaftSnapshotHeaderCreator());
        registerBodyCreator(SingleSnapshotBody.class, new SingleSnapshotBodyCreator());
        registerBodyCreator(RaftSnapshotBody.class, new RaftSnapshotBodyCreator());
        registerFooterCreator(SingleSnapshotFooter.class, new SingleSnapshotFooterCreator());
        registerFooterCreator(RaftSnapshotFooter.class, new RaftSnapshotFooterCreator());
        registerHeaderDeserializer(SingleSnapshotHeader.class, new SingleSnapshotHeaderDeserializer());
        registerHeaderDeserializer(RaftSnapshotHeader.class, new RaftSnapshotHeaderDeserializer());
        registerBodyDeserializer(SingleSnapshotBody.class, new SingleSnapshotBodyDeserializer());
        registerBodyDeserializer(RaftSnapshotBody.class, new RaftSnapshotBodyDeserializer());
        registerFooterDeserializer(SingleSnapshotFooter.class, new SingleSnapshotFooterDeserializer());
        registerFooterDeserializer(RaftSnapshotFooter.class, new RaftSnapshotFooterDeserializer());
    }

    public static void registerHeaderDeserializer(
            final Class<? extends SnapshotHeader> clazz,
            final SnapshotHeaderDeserializer deserializer) {
        headerDeserializerRegistry.put(clazz, deserializer);
    }

    public static <T extends SnapshotHeader> T deserializeHeader(
            final Class<T> clazz, final ByteBuffer buffer) {

        final SnapshotHeaderDeserializer deserializer = headerDeserializerRegistry.get(clazz);

        if (deserializer == null) {
            throw new NotFoundException("SnapshotHeader deserializer for " + clazz.getName() + " not registered");
        }

        return clazz.cast(deserializer.deserialize(buffer));
    }

    public static void registerBodyDeserializer(
            final Class<? extends SnapshotBody> clazz,
            final SnapshotBodyDeserializer deserializer) {
        bodyDeserializerRegistry.put(clazz, deserializer);
    }

    public static <T extends SnapshotBody> T deserializeBody(
            final Class<T> clazz, final ByteBuffer buffer) {

        final SnapshotBodyDeserializer deserializer = bodyDeserializerRegistry.get(clazz);

        if (deserializer == null) {
            throw new NotFoundException("Body deserializer for " + clazz.getName() + " not registered");
        }

        return clazz.cast(deserializer.deserialize(buffer));
    }

    public static void registerFooterDeserializer(
            final Class<? extends SnapshotFooter> clazz,
            final SnapshotFooterDeserializer deserializer) {
        footerDeserializerRegistry.put(clazz, deserializer);
    }

    public static <T extends SnapshotFooter> T deserializeFooter(
            final Class<T> clazz, final ByteBuffer buffer) {

        final SnapshotFooterDeserializer deserializer = footerDeserializerRegistry.get(clazz);

        if (deserializer == null) {
            throw new NotFoundException("Footer deserializer for " + clazz.getName() + " not registered");
        }

        return clazz.cast(deserializer.deserialize(buffer));
    }

    public static void registerHeaderCreator(
            final Class<? extends SnapshotHeader> clazz,
            final SnapshotHeaderCreator<?> creator) {
        headerCreatorRegistry.put(clazz, creator);
    }

    public static <T extends SnapshotHeader, L extends LogEntry> T createHeader(
            final Class<T> clazz, final List<L> entries) {

        final SnapshotHeaderCreator<L> creator =
                (SnapshotHeaderCreator<L>) headerCreatorRegistry.get(clazz);

        if (creator == null) {
            throw new NotFoundException("SnapshotHeader creator for " + clazz.getName() + " not registered");
        }

        return clazz.cast(creator.create(entries));
    }

    public static void registerBodyCreator(
            final Class<? extends SnapshotBody> clazz,
            final SnapshotBodyCreator<?> creator) {
        bodyCreatorRegistry.put(clazz, creator);
    }

    public static <T extends SnapshotBody, L extends LogEntry> T createBody(
            final Class<T> clazz, final List<L> entries) {

        final SnapshotBodyCreator<L> creator =
                (SnapshotBodyCreator<L>) bodyCreatorRegistry.get(clazz);

        if (creator == null) {
            throw new NotFoundException("Body creator for " + clazz.getName() + " not registered");
        }

        return clazz.cast(creator.create(entries));
    }

    public static void registerFooterCreator(
            final Class<? extends SnapshotFooter> clazz,
            final SnapshotFooterCreator<?> creator) {
        footerCreatorRegistry.put(clazz, creator);
    }

    public static <T extends SnapshotFooter, L extends LogEntry> T createFooter(
            final Class<T> clazz, final List<L> entries) {

        final SnapshotFooterCreator<L> creator =
                (SnapshotFooterCreator<L>) footerCreatorRegistry.get(clazz);

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