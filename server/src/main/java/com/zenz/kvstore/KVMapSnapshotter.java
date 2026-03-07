package com.zenz.kvstore;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class KVMapSnapshotter {
    private static final Path DEFAULT_SNAPSHOT_DIR = Path.of("snapshots");

    private Path dir = DEFAULT_SNAPSHOT_DIR;

    public KVMapSnapshotter() throws IOException {
        ensureDir();
    }

    public KVMapSnapshotter(Path dir) throws IOException {
        this.dir = dir;
        ensureDir();
    }

    private void ensureDir() throws IOException {
        File folder = dir.toFile();
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }

    public void snapshot(KVMap map, Path fpath) throws IOException {
        if (!Files.exists(fpath)) Files.createFile(fpath);

        try (FileChannel channel = FileChannel.open(fpath, StandardOpenOption.WRITE)) {
            writeNodes(channel, map);
            channel.force(true);
        }
    }

    public void writeNodes(FileChannel channel, KVMap map) throws IOException {
        KVArray arr = map.getHt2();
        if (arr != null) {
            for (int i = 0; i < arr.length(); i++) {
                KVMap.NodeList nodeList = arr.get(i);
                if (nodeList != null) {
                    KVMap.Node node = nodeList.head;
                    while (node != null) {
                        serializeNode(channel, node);
                        node = node.next;
                    }
                }
            }
        }

        arr = map.getHt1();
        if (arr != null) {
            for (int i = 0; i < arr.length(); i++) {
                KVMap.NodeList nodeList = arr.get(i);
                if (nodeList != null) {
                    KVMap.Node node = nodeList.head;
                    while (node != null) {
                        serializeNode(channel, node);
                        node = node.next;
                    }
                }
            }
        }
    }

    private void serializeNode(FileChannel channel, KVMap.Node node) throws IOException {
        byte[] keyBytes = node.key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + keyBytes.length + 4 + node.value.length + 1);

        buffer.putInt(buffer.capacity());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(node.value.length);
        buffer.put(node.value);
        buffer.put(ByteBuffer.wrap("\n".getBytes(StandardCharsets.UTF_8)));

        buffer.flip();
        channel.write(buffer);
    }

    public KVMap loadSnapshot() throws IOException {
        File[] files = dir.toFile().listFiles();
        if (files == null || files.length == 0) return null;
        return loadSnapshot(files[0].toPath());
    }

    public KVMap loadSnapshot(Path fpath) throws IOException {
        if (!Files.exists(fpath)) return null;

        KVMap map = new KVMap();

        byte[] fBytes = Files.readAllBytes(fpath);
        ByteBuffer buffer = ByteBuffer.wrap(fBytes);

        while (buffer.hasRemaining()) {
            KVPair pair = deserializeNode(buffer);
            map.put(pair.key, pair.value);
        }

        return map;
    }

    private KVPair deserializeNode(ByteBuffer buffer) throws IOException {
        int bufferLength = buffer.getInt();
        byte[] nodeBytes = new byte[bufferLength - 4];
        buffer.get(nodeBytes);
        ByteBuffer nodeBuffer = ByteBuffer.wrap(nodeBytes);

        int keyLength = nodeBuffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        nodeBuffer.get(keyBytes);

        int valueLength = nodeBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        nodeBuffer.get(valueBytes);

        return new KVPair(new String(keyBytes, StandardCharsets.UTF_8), valueBytes);
    }

    public Path getDir() {
        return dir;
    }

    public static record KVPair(String key, byte[] value) {
    }
}
