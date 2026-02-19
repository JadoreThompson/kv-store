package com.zenz.kvstore;

import com.zenz.kvstore.operations.GetOperation;
import com.zenz.kvstore.operations.Operation;
import com.zenz.kvstore.operations.PutOperation;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class KVSnapshotter {
    private static final String DEFAULT_SNAPSHOT_FOLDER_NAME = "snapshots";
    private static String HEADER_START = "===HEADER START===";
    private static String HEADER_END = "===HEADER END===";
    private static String KVPAIRS_START = "===KV START===";
    private static String KVPAIRS_END = "===KV END===";
    private final File folder;

    public KVSnapshotter() throws IOException {
        folder = new File(DEFAULT_SNAPSHOT_FOLDER_NAME);
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }

    public KVSnapshotter(String folderName) throws IOException {
        folder = new File(folderName);
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }

    /**
     * Creates a compounded snapshot of KVStore stemming
     * from the most recent snapshot.
     *
     * @param fname - Name of the file containing logs
     */
    public void snapshot(String fname) throws IOException {
        String prefix = extractPrefix(fname);
        String contents = Files.readString(Path.of(fname));
        String[] lines = contents.strip().split("\n");

        // Load previous store
        KVStore store;
        if (!prefix.equals("0")) {
            long prevSnapshot = Long.parseLong(prefix) - 1;
            String prevSnapshotFname = folder.getPath() + "/" + prevSnapshot + ".snapshot";
            store = loadSnapshot(prevSnapshotFname);
        } else {
            store = new KVStore(Files.createTempDirectory("tmp-").toString(), false);
        }

        // Apply the logged operations
        for (String line : lines) {
            if (line.isBlank()) continue;

            Operation operation = Operation.fromLine(line);
            if (operation.type().equals(OperationType.PUT)) {
                PutOperation putOp = (PutOperation) operation;
                store.put(putOp.key(), putOp.value());
            } else if (operation.type().equals(OperationType.GET)) {
                store.get(((GetOperation) operation).key());
            } else {
                throw new UnsupportedOperationException("Unsupported operation + " + operation.type());
            }
        }

        // Save snapshot
        saveSnapshot(folder.getPath() + "/" + prefix + ".snapshot", store);
    }

    private String extractPrefix(String fname) {
        // Extract the number prefix from filename like "0.log" or "/path/to/0.log"
        String name = fname;
        if (fname.contains("/")) {
            name = fname.substring(fname.lastIndexOf("/") + 1);
        }
        if (fname.contains("\\")) {
            name = fname.substring(fname.lastIndexOf("\\") + 1);
        }
        return name.replace(".log", "");
    }

    private void saveSnapshot(String fname, KVStore store) throws IOException {
        try (FileWriter writer = new FileWriter(fname)) {
            writeHeader(writer, store);
            writeNodes(writer, store);
        }
    }

    private void writeHeader(FileWriter writer, KVStore store) throws IOException {
        writer.write(HEADER_START + "\n");
        writer.write("1\n");  // version
        writer.write(HEADER_END + "\n");
    }

    private void writeNodes(FileWriter writer, KVStore store) throws IOException {
        KVMap map = store.getMap();

        writer.write(KVPAIRS_START + "\n");

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

        writer.write(KVPAIRS_END + "\n");
    }

    public KVStore loadSnapshot(String fpath) throws IOException {
        Path tmpDir = Files.createTempDirectory("tmp-");
        KVStore store = new KVStore(tmpDir.toString(), false);

        try (BufferedReader reader = new BufferedReader(new FileReader(fpath))) {
            String line;

            while ((line = reader.readLine()) != null) {
                Header header = loadHeader(reader);
                ArrayList<KVPair> pairs = loadKVPairs(reader);
                if (pairs != null) {
                    for (KVPair pair : pairs) {
                        store.put(pair.key, pair.value);
                    }
                }
            }
        }

        return store;
    }

    public KVMap loadSnapshotV2() throws IOException {
        File[] files = folder.listFiles();
        if (files == null || files.length == 0) return null;

        File recentSnapshot = null;
        for (int i = 0; i < files.length; i++) {
            if (files[i + 1] == null) {
                recentSnapshot = files[i];
                break;
            }
        }

        return loadSnapshotV2(recentSnapshot.getAbsolutePath());
    }

    public KVMap loadSnapshotV2(String fpath) throws IOException {
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
        if (line == null || !line.equals(KVPAIRS_START)) {
            return null;
        }

        ArrayList<KVPair> pairs = new ArrayList<>();

        while (true) {
            line = reader.readLine();

            if (line == null) return null;
            if (line.equals(KVPAIRS_END)) return pairs;

            String[] components = line.strip().split(" ");
            KVPair pair = new KVPair(components[0], components[1].getBytes(StandardCharsets.UTF_8));
            pairs.add(pair);
        }
    }

    public static record Header(int version) {
    }

    private static record KVPair(String key, byte[] value) {
    }
}