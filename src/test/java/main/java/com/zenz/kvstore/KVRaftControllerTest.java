package main.java.com.zenz.kvstore;

import com.zenz.kvstore.*;
import com.zenz.kvstore.raft.KVRaftController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVRaftControllerTest {
    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 10099;

    private Path logsFolder;
    private Path snapshotsFolder;

    private KVRaftStore store;
    private KVMapSnapshotter snapshotter;
    private WALogger logger;

    private KVRaftController controller;
    private KVConnectionManager connectionManager;
    private KVRaftControllerCommandProcessor commandProcessor;

    private ExecutorService controllerExecutor;
    private ExecutorService connectionManagerExecutor;

    @BeforeEach
    void setUp() throws Exception {
        logsFolder = Files.createTempDirectory("tmp-raft-logs-");
        snapshotsFolder = Files.createTempDirectory("tmp-raft-snapshots-");
        snapshotter = new KVMapSnapshotter(snapshotsFolder);
        store = new KVRaftStore.Builder()
                .setLogsFolder(logsFolder)
                .setSnapshotter(snapshotter)
                .setSnapshotEnabled(false)
                .build();
        logger = getLogger(store);

        // Set up the controller and connection manager
        controller = new KVRaftController(store, TEST_HOST, TEST_PORT + 1);
        commandProcessor = new KVRaftControllerCommandProcessor(store, controller);
        connectionManager = new KVConnectionManager(TEST_HOST, TEST_PORT, commandProcessor);

        // Start controller on background thread
        controllerExecutor = Executors.newSingleThreadExecutor();
        controllerExecutor.submit(() -> {
            try {
                controller.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Start connection manager on background thread
        connectionManagerExecutor = Executors.newSingleThreadExecutor();
        connectionManagerExecutor.submit(() -> {
            try {
                connectionManager.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Wait for servers to start
        Thread.sleep(500);
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException {
        // Stop connection manager
        if (connectionManager != null) {
            connectionManager.stop();
        }
        if (connectionManagerExecutor != null) {
            connectionManagerExecutor.shutdown();
            connectionManagerExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }

        // Stop controller
        if (controller != null) {
            controller.stop();
        }
        if (controllerExecutor != null) {
            controllerExecutor.shutdown();
            controllerExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }

        if (logger != null) {
            logger.close();
        }

        logsFolder.toFile().delete();
        snapshotsFolder.toFile().delete();
    }

    private WALogger getLogger(KVRaftStore store) throws Exception {
        Field loggerField = KVStore.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        return (WALogger) loggerField.get(store);
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
        String response = receiveMessage(client);

        assertEquals("OK", response);

        // Verify via GET
        sendMessage(client, "GET mykey");
        String getResponse = receiveMessage(client);

        assertEquals("OK myvalue", getResponse);
        client.close();
    }

    // --- GET Tests ---

    @Test
    void get_existingKey_returnsValue() throws IOException {
        SocketChannel client = connectClient();

        // First PUT a value
        sendMessage(client, "PUT existingkey existingvalue");
        receiveMessage(client);  // consume OK

        // Then GET it
        sendMessage(client, "GET existingkey");
        String response = receiveMessage(client);

        assertEquals("OK existingvalue", response);
        client.close();
    }
}
