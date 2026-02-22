package com.zenz.kvstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class KVMapSnapshotter {
    private static final Path DEFAULT_SNAPSHOT_FOLDER_Path = Path.of("snapshots");
    private static final String HEADER_START = "===HEADER START===";
    private static final String HEADER_END = "===HEADER END===";
    private static final String KV_START = "===KV START===";
    private static final String KV_END = "===KV END===";

    private final Path folderPath;
    private boolean multiThreadingEnabled;

    public KVMapSnapshotter() throws IOException {
        folderPath = DEFAULT_SNAPSHOT_FOLDER_Path;
        File folder = folderPath.toFile();
        if (!folder.exists()) {
            folder.mkdirs();
        }

        multiThreadingEnabled = true;
    }

    public KVMapSnapshotter(Path folderPath) throws IOException {
        this.folderPath = folderPath;
        File folder = folderPath.toFile();
        if (!folder.exists()) {
            folder.mkdirs();
        }

        multiThreadingEnabled = true;
    }

    public Kryo getKryo() {
        Kryo kryo = new Kryo();

        kryo.register(KVMap.class);
        kryo.register(KVArray.class);
        kryo.register(KVMap.NodeList[].class);
        kryo.register(KVMap.NodeList.class);
        kryo.register(KVMap.Node.class);
        kryo.register(String.class);
        kryo.register(byte[].class);

        return kryo;
    }

    /**
     * Creates a snapshot of the map
     */
    public void snapshot(KVMap map) throws IOException {
        Kryo kryo = getKryo();

        Path fpath = Files.createTempFile("tmp-snapshot-", ".snapshot");
        try (Output output = new Output(new FileOutputStream(fpath.toString()))) {
            kryo.writeObject(output, map);
        }

        Path snapshotFpath = getSnapshotFpath();
        if (snapshotFpath == null) throw new RuntimeException("Failed to find path for snapshot file");

        if (multiThreadingEnabled) {
            Thread th = new Thread(() -> {
                try {
                    saveSnapshot(fpath, snapshotFpath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            th.start();
        } else {
            saveSnapshot(fpath, snapshotFpath);
        }
    }

    private Path getSnapshotFpath() throws IOException {
        File[] files = folderPath.toFile().listFiles();
        Path fpath;
        if (files == null || files.length == 0) {
            fpath = folderPath.resolve("0.snapshot");
            fpath.toFile().createNewFile();
            return fpath;
        }
        ;

        for (int i = 0; i < files.length; i++) {
            if (i + 1 >= files.length || files[i + 1] == null) {
                fpath = folderPath.resolve(i + 1 + ".snapshot");
                fpath.toFile().createNewFile();
                return fpath;
            }
        }

        return null;
    }

    private void saveSnapshot(Path serialisedMapPath, Path snapshotFpath) throws IOException {
        try (Input input = new Input(new FileInputStream(serialisedMapPath.toString()))) {
            Kryo kryo = getKryo();
            KVMap map = kryo.readObject(input, KVMap.class);

            try (FileWriter writer = new FileWriter(snapshotFpath.toString())) {
                writeHeader(writer, map);
                writeNodes(writer, map);
                writer.flush();
            }
        }
    }

    private void writeHeader(FileWriter writer, KVMap map) throws IOException {
        writer.write(HEADER_START + "\n");
        writer.write("1\n");  // version
        writer.write(HEADER_END + "\n");
    }

    private void writeNodes(FileWriter writer, KVMap map) throws IOException {
        writer.write(KV_START + "\n");

        if (map.getHt2() != null) {
            for (int i = 0; i < map.getHt2().length(); i++) {
                KVMap.NodeList nodeList = map.getHt2().get(i);
                if (nodeList != null) {
                    KVMap.Node node = nodeList.head;
                    while (node != null) {
                        writer.write(node.key + " " + new String(node.value, StandardCharsets.UTF_8) + "\n");
                        node = node.next;
                    }
                }
            }
        }

        if (map.getHt1() != null) {
            for (int i = 0; i < map.getHt1().length(); i++) {
                KVMap.NodeList nodeList = map.getHt1().get(i);
                if (nodeList != null) {
                    KVMap.Node node = nodeList.head;
                    while (node != null) {
                        writer.write(node.key + " " + new String(node.value, StandardCharsets.UTF_8) + "\n");
                        node = node.next;
                    }
                }
            }
        }

        writer.write(KV_END + "\n");
    }

    public KVMap loadSnapshot() throws IOException {
        File[] files = folderPath.toFile().listFiles();
        if (files == null || files.length == 0) return null;

        for (int i = 0; i < files.length; i++) {
            if (i + 1 >= files.length || files[i + 1] == null) {
                return loadSnapshot(files[i].getAbsolutePath());
            }
        }

        return null;
    }

    private KVMap loadSnapshot(String fpath) throws IOException {
        KVMap map = new KVMap();

        try (BufferedReader reader = new BufferedReader(new FileReader(fpath))) {
            Header header = loadHeader(reader);
            ArrayList<KVPair> pairs = loadKVPairs(reader);

            if (pairs == null) return null;
            for (KVPair pair : pairs) {
                map.put(pair.key, pair.value);
            }
        }

        return map;
    }

    private Header loadHeader(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null || !line.equals(HEADER_START)) {
            return null;
        }

        line = reader.readLine();
        if (line == null) return null;

        int version = Integer.parseInt(line.strip());
        Header header = new Header(version);

        line = reader.readLine();
        if (line == null || !line.equals(HEADER_END)) {
            return null;
        }

        return header;
    }

    private ArrayList<KVPair> loadKVPairs(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null || !line.equals(KV_START)) {
            return null;
        }

        ArrayList<KVPair> pairs = new ArrayList<>();

        while (true) {
            line = reader.readLine();

            if (line == null) return null;
            if (line.equals(KV_END)) return pairs;

            String[] components = line.strip().split(" ");
            KVPair pair = new KVPair(components[0], components[1].getBytes(StandardCharsets.UTF_8));
            pairs.add(pair);
        }
    }

    public Path getFolderPath() {
        return folderPath;
    }

    public boolean isMultiThreadingEnabled() {
        return multiThreadingEnabled;
    }

    public void setMultiThreadingEnabled(boolean multiThreadingEnabled) {
        this.multiThreadingEnabled = multiThreadingEnabled;
    }

    public static record Header(int version) {
    }

    private static record KVPair(String key, byte[] value) {
    }
}
