package com.zenz.kvstore;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KVConnectionManagerTest {

    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 9999;

    private static ExecutorService serverExecutor;
    private static KVConnectionManager server;
    private static Path tempDir;

    @BeforeAll
    static void startServer() throws IOException, InterruptedException {
        tempDir = Files.createTempDirectory("kvstore-test-");
        Path logsFolderPath = tempDir.resolve("logs");
        Path snapshotFolderPath = tempDir.resolve("snapshots");

        KVMapSnapshotter snapshotter = new KVMapSnapshotter(snapshotFolderPath);
        KVStore store = new KVStore.Builder()
                .setLogsFolder(logsFolderPath)
                .setSnapshotter(snapshotter)
                .setSnapshotEnabled(false)
                .build();
        server = new KVConnectionManager(TEST_HOST, TEST_PORT, store);

        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            try {
                server.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Wait for server to start
        Thread.sleep(500);
    }

    @AfterAll
    static void stopServer() throws IOException {
        server.stop();
        serverExecutor.shutdown();
        try {
            serverExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        tempDir.toFile().delete();
    }

    private SocketChannel connectClient() throws IOException {
        SocketChannel client = SocketChannel.open();
        client.configureBlocking(true);
        client.connect(new InetSocketAddress(TEST_HOST, TEST_PORT));
        return client;
    }

    private void sendMessage(SocketChannel client, String message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap((message + "\n").getBytes(StandardCharsets.UTF_8));
        while (buffer.hasRemaining()) {
            client.write(buffer);
        }
    }

    private String receiveMessage(SocketChannel client) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int bytesRead = client.read(buffer);
        if (bytesRead <= 0) {
            return null;
        }
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new String(data, StandardCharsets.UTF_8).trim();
    }

    // --- Connection Tests ---

    @Test
    void clientCanConnect() throws IOException {
        SocketChannel client = connectClient();
        assertTrue(client.isConnected(), "Client should be connected");
        client.close();
    }

    @Test
    void multipleClientsCanConnect() throws IOException {
        SocketChannel client1 = connectClient();
        SocketChannel client2 = connectClient();
        SocketChannel client3 = connectClient();

        assertTrue(client1.isConnected());
        assertTrue(client2.isConnected());
        assertTrue(client3.isConnected());

        client1.close();
        client2.close();
        client3.close();
    }

    // --- PUT Tests ---

    @Test
    void put_returnsOk() throws IOException {
        SocketChannel client = connectClient();
        sendMessage(client, "PUT testkey testvalue");
        String response = receiveMessage(client);

        assertEquals("OK", response);
        client.close();
    }

    @Test
    void put_storesInMap() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "PUT mykey myvalue");
        receiveMessage(client);  // consume OK

        // Verify via GET
        sendMessage(client, "GET mykey");
        String response = receiveMessage(client);

        assertEquals("OK myvalue", response);
        client.close();
    }

    @Test
    void put_withSpacesInValue() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "PUT greeting hello world");
        receiveMessage(client);  // consume OK

        sendMessage(client, "GET greeting");
        String response = receiveMessage(client);

        assertEquals("OK hello world", response);
        client.close();
    }

    @Test
    void put_missingKey_returnsError() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "PUT");
        String response = receiveMessage(client);

        assertTrue(response.contains("ERROR"));
        client.close();
    }

    @Test
    void put_missingValue_returnsError() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "PUT onlykey");
        String response = receiveMessage(client);

        assertTrue(response.contains("ERROR"));
        client.close();
    }

    // --- GET Tests ---

    @Test
    void get_existingKey_returnsValue() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "PUT existingkey existingvalue");
        receiveMessage(client);  // consume OK

        sendMessage(client, "GET existingkey");
        String response = receiveMessage(client);

        assertEquals("OK existingvalue", response);
        client.close();
    }

    @Test
    void get_missingKey_returnsNull() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "GET nonexistentkey");
        String response = receiveMessage(client);

        assertEquals("NULL", response);
        client.close();
    }

    @Test
    void get_missingKey_returnsError() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "GET");
        String response = receiveMessage(client);

        assertTrue(response.contains("ERROR"));
        client.close();
    }

    // --- PING Tests ---

    @Test
    void ping_returnsPong() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "PING");
        String response = receiveMessage(client);

        assertEquals("PONG", response);
        client.close();
    }

    // --- Unknown Operation Tests ---

    @Test
    void unknownOperation_returnsError() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "DELETE something");
        String response = receiveMessage(client);

        assertTrue(response.contains("ERROR"));
        client.close();
    }

    // --- Multiple Operations Tests ---

    @Test
    void multipleOperations_sameConnection() throws IOException {
        SocketChannel client = connectClient();

        // PUT
        sendMessage(client, "PUT key1 value1");
        assertEquals("OK", receiveMessage(client));

        // GET
        sendMessage(client, "GET key1");
        assertEquals("OK value1", receiveMessage(client));

        // PUT overwrite
        sendMessage(client, "PUT key1 newvalue");
        assertEquals("OK", receiveMessage(client));

        // GET updated
        sendMessage(client, "GET key1");
        assertEquals("OK newvalue", receiveMessage(client));

        client.close();
    }

    @Test
    void multipleKeys_allStored() throws IOException {
        SocketChannel client = connectClient();

        // Store multiple keys
        sendMessage(client, "PUT keyA valueA");
        assertEquals("OK", receiveMessage(client));

        sendMessage(client, "PUT keyB valueB");
        assertEquals("OK", receiveMessage(client));

        sendMessage(client, "PUT keyC valueC");
        assertEquals("OK", receiveMessage(client));

        // Retrieve all
        sendMessage(client, "GET keyA");
        assertEquals("OK valueA", receiveMessage(client));

        sendMessage(client, "GET keyB");
        assertEquals("OK valueB", receiveMessage(client));

        sendMessage(client, "GET keyC");
        assertEquals("OK valueC", receiveMessage(client));

        client.close();
    }

    // --- Map Update Verification ---

    @Test
    void mapIsUpdated_afterPut() throws IOException {
        SocketChannel client = connectClient();

        sendMessage(client, "PUT maptestkey maptestvalue");
        receiveMessage(client);

        // Verify directly in store
        KVMap.Node node = server.getStore().getMap().get("maptestkey");
        assertNotNull(node, "Key should exist in map");
        assertEquals("maptestvalue", new String(node.value, StandardCharsets.UTF_8));

        client.close();
    }
}