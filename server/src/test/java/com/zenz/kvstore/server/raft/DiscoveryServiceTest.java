package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.client.KVStoreClient;
import com.zenz.kvstore.common.response.GetResponse;
import com.zenz.kvstore.common.response.SearchResponse;
import com.zenz.kvstore.common.utils.Utils;
import com.zenz.kvstore.server.KVMapSnapshotter;
import com.zenz.kvstore.server.KVServer;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.command.handler.CommandHandler;
import com.zenz.kvstore.server.logging.WALogger;
import com.zenz.kvstore.server.logging.LogHandler;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.contains;

/**
 * Comprehensive test class for {@link DiscoveryService}.
 */
@ExtendWith(MockitoExtension.class)
class DiscoveryServiceTest {

    private static final UUID CLUSTER_TOKEN = UUID.randomUUID();

    @Mock
    private KVStoreClient mockClient;

    @Mock
    private GetResponse mockGetResponse;

    @Mock
    private SearchResponse mockSearchResponse;

    @Mock
    private SearchResponse.Entry mockEntry1;

    @Mock
    private SearchResponse.Entry mockEntry2;

    private static final Random random = new Random();

    private DiscoveryService discoveryService;

    private KVServer kvServer;

    private Thread kvServerThread;

    @BeforeEach
    void setUp() throws Exception {
        final Path snapshotsDir = Files.createTempDirectory("test-snapshots-");
        final Path logsDir = Files.createTempDirectory("test-logs-");
        final KVStore kvStore = new KVStore(
                new KVStore.Builder()
                        .setSnapshotter(new KVMapSnapshotter(snapshotsDir))
                        .setLogHandler(new LogHandler(new WALogger(logsDir.resolve("test.log"))))
        );

        this.kvServer = new KVServer(
                "localhost", random.nextInt(1000, 9999), new CommandHandler(kvStore));
        this.kvServerThread = new Thread(() -> Utils.checkedRunnableWrapper(kvServer::start));
        this.kvServerThread.start();

        for (int i = 0; i < 5; i++) {
            if (kvServer.isRunning()) {
                break;
            }

            Thread.sleep(100);
        }

        if (!kvServer.isRunning()) {
            throw new RuntimeException("KV Server is not running");
        }

        this.discoveryService = new DiscoveryService(
                CLUSTER_TOKEN, new InetSocketAddress(kvServer.getHost(), kvServer.getPort()));

        injectMockClient(discoveryService, mockClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        this.discoveryService.disconnect();
        this.kvServer.stop();
        this.kvServerThread.interrupt();
    }

    private void injectMockClient(DiscoveryService service, KVStoreClient client) throws Exception {
        Field clientField = DiscoveryService.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(service, client);
    }

    // ==================== STATIC METHOD TESTS ====================

    @Nested
    @DisplayName("Static URL Methods Tests")
    class StaticUrlMethodsTests {

        @Test
        @DisplayName("getClusterUrl should return correct cluster URL format")
        void getClusterUrl_shouldReturnCorrectFormat() {
            UUID token = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
            String result = DiscoveryService.getClusterUrl(token);

            assertEquals("cluster/123e4567-e89b-12d3-a456-426614174000/", result);
        }

        @Test
        @DisplayName("getClusterUrl should handle different UUIDs")
        void getClusterUrl_shouldHandleDifferentUuids() {
            UUID token1 = UUID.randomUUID();
            UUID token2 = UUID.randomUUID();

            String result1 = DiscoveryService.getClusterUrl(token1);
            String result2 = DiscoveryService.getClusterUrl(token2);

            assertEquals("cluster/" + token1 + "/", result1);
            assertEquals("cluster/" + token2 + "/", result2);
            assertNotEquals(result1, result2);
        }

        @Test
        @DisplayName("getClusterMemberUrl should return correct member URL format")
        void getClusterMemberUrl_shouldReturnCorrectFormat() {
            UUID token = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
            String result = DiscoveryService.getClusterMemberUrl(token, "node1");

            assertEquals("cluster/123e4567-e89b-12d3-a456-426614174000/members/node1", result);
        }

        @Test
        @DisplayName("getClusterMemberUrl should handle special characters in member name")
        void getClusterMemberUrl_shouldHandleSpecialCharacters() {
            UUID token = UUID.randomUUID();
            String result = DiscoveryService.getClusterMemberUrl(token, "node-123");

            assertTrue(result.contains("node-123"));
            assertTrue(result.endsWith("/members/node-123"));
        }

        @Test
        @DisplayName("getClusterMemberUrl should handle empty member name")
        void getClusterMemberUrl_shouldHandleEmptyMemberName() {
            UUID token = UUID.randomUUID();
            String result = DiscoveryService.getClusterMemberUrl(token, "");

            assertTrue(result.endsWith("/members/"));
        }
    }

    // ==================== ADD MEMBER TESTS ====================

    @Nested
    @DisplayName("addMember() Method Tests")
    class AddMemberTests {

        @Test
        @DisplayName("addMember should serialize and store node config")
        void addMember_shouldSerializeAndStore() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "node1",
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 8080));

            discoveryService.addMember(CLUSTER_TOKEN, nodeConfig);

            verify(mockClient).put(eq("cluster/" + CLUSTER_TOKEN + "/members/node1"), any(byte[].class));
        }

        @Test
        @DisplayName("addMember should use correct cluster token")
        void addMember_shouldUseCorrectClusterToken() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "node1",
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 8080));
            UUID differentToken = UUID.fromString("999e4567-e89b-12d3-a456-426614174999");

            discoveryService.addMember(differentToken, nodeConfig);

            verify(mockClient).put(eq("cluster/" + differentToken + "/members/node1"), any(byte[].class));
        }

        @Test
        @DisplayName("addMember should handle IOException from client")
        void addMember_shouldHandleIOException() throws Exception {
            NodeConfig nodeConfig = new NodeConfig("node1", new InetSocketAddress("192.168.1.1", 9999), new InetSocketAddress("192.168.1.1", 8080));
            when(mockClient.put(anyString(), any(byte[].class))).thenThrow(new IOException("Connection failed"));

            assertThrows(IOException.class, () ->
                    discoveryService.addMember(CLUSTER_TOKEN, nodeConfig)
            );
        }

        @Test
        @DisplayName("addMember should handle InterruptedException from client")
        void addMember_shouldHandleInterruptedException() throws Exception {
            NodeConfig nodeConfig = new NodeConfig("node1", new InetSocketAddress("192.168.1.1", 9999), new InetSocketAddress("192.168.1.1", 8080));
            when(mockClient.put(anyString(), any(byte[].class))).thenThrow(new InterruptedException());

            assertThrows(InterruptedException.class, () ->
                    discoveryService.addMember(CLUSTER_TOKEN, nodeConfig)
            );
        }

        @Test
        @DisplayName("addMember should serialize node config correctly")
        void addMember_shouldSerializeNodeConfigCorrectly() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "testNode",
                            new InetSocketAddress("10.0.0.1", 9999),
                            new InetSocketAddress("10.0.0.1", 9090));
            byte[] expectedSerialized = nodeConfig.serialize();

            discoveryService.addMember(CLUSTER_TOKEN, nodeConfig);

            verify(mockClient).put(anyString(), eq(expectedSerialized));
        }
    }

    // ==================== DELETE MEMBER TESTS ====================

    @Nested
    @DisplayName("deleteMember() Method Tests")
    class DeleteMemberTests {

        @Test
        @DisplayName("deleteMember should call client delete with correct URL")
        void deleteMember_shouldCallDeleteWithCorrectUrl() throws Exception {
            discoveryService.deleteMember(CLUSTER_TOKEN, "node1");

            verify(mockClient).delete("cluster/" + CLUSTER_TOKEN + "/members/node1");
        }

        @Test
        @DisplayName("deleteMember should use provided cluster token")
        void deleteMember_shouldUseProvidedClusterToken() throws Exception {
            UUID otherToken = UUID.fromString("888e4567-e89b-12d3-a456-426614174888");

            discoveryService.deleteMember(otherToken, "node1");

            verify(mockClient).delete("cluster/" + otherToken + "/members/node1");
        }

        @Test
        @DisplayName("deleteMember should handle IOException")
        void deleteMember_shouldHandleIOException() throws Exception {
            when(mockClient.delete(anyString())).thenThrow(new IOException("Connection failed"));

            assertThrows(IOException.class, () ->
                    discoveryService.deleteMember(CLUSTER_TOKEN, "node1")
            );
        }

        @Test
        @DisplayName("deleteMember should handle InterruptedException")
        void deleteMember_shouldHandleInterruptedException() throws Exception {
            when(mockClient.delete(anyString())).thenThrow(new InterruptedException());

            assertThrows(InterruptedException.class, () ->
                    discoveryService.deleteMember(CLUSTER_TOKEN, "node1")
            );
        }

        @Test
        @DisplayName("deleteMember should handle empty member name")
        void deleteMember_shouldHandleEmptyMemberName() throws Exception {
            discoveryService.deleteMember(CLUSTER_TOKEN, "");

            verify(mockClient).delete("cluster/" + CLUSTER_TOKEN + "/members/");
        }
    }

    // ==================== GET MEMBER TESTS ====================

    @Nested
    @DisplayName("getMember() Method Tests")
    class GetMemberTests {

        @Test
        @DisplayName("getMember should return deserialized NodeConfig")
        void getMember_shouldReturnDeserializedNodeConfig() throws Exception {
            NodeConfig expectedConfig =
                    new NodeConfig(
                            "node1",
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 8080));
            byte[] serializedConfig = expectedConfig.serialize();

            when(mockGetResponse.value()).thenReturn(serializedConfig);
            when(mockClient.get(anyString())).thenReturn(mockGetResponse);

            NodeConfig result = discoveryService.getMember(CLUSTER_TOKEN, "node1");

            assertNotNull(result);
            assertEquals("node1", result.name());
            assertEquals("192.168.1.1", result.serverAddress().getAddress().getHostAddress());
            assertEquals(9999, result.serverAddress().getPort());
        }

        @Test
        @DisplayName("getMember should use correct URL")
        void getMember_shouldUseCorrectUrl() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "node1",
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 8080));
            when(mockGetResponse.value()).thenReturn(nodeConfig.serialize());
            when(mockClient.get(anyString())).thenReturn(mockGetResponse);

            discoveryService.getMember(CLUSTER_TOKEN, "node1");

            verify(mockClient).get("cluster/" + CLUSTER_TOKEN + "/members/node1");
        }

        @Test
        @DisplayName("getMember should handle IOException")
        void getMember_shouldHandleIOException() throws Exception {
            when(mockClient.get(anyString())).thenThrow(new IOException("Connection failed"));

            assertThrows(IOException.class, () ->
                    discoveryService.getMember(CLUSTER_TOKEN, "node1")
            );
        }

        @Test
        @DisplayName("getMember should handle InterruptedException")
        void getMember_shouldHandleInterruptedException() throws Exception {
            when(mockClient.get(anyString())).thenThrow(new InterruptedException());

            assertThrows(InterruptedException.class, () ->
                    discoveryService.getMember(CLUSTER_TOKEN, "node1")
            );
        }
    }

    // ==================== GET MEMBERS TESTS ====================

    @Nested
    @DisplayName("getMembers() Method Tests")
    class GetMembersTests {

        @Test
        @DisplayName("getMembers should return list of NodeConfigs")
        void getMembers_shouldReturnListOfNodeConfigs() throws Exception {
            NodeConfig node1 = new NodeConfig("node1", new InetSocketAddress("192.168.1.1", 9999), new InetSocketAddress("192.168.1.1", 8080));
            NodeConfig node2 = new NodeConfig("node2", new InetSocketAddress("192.168.1.2", 9999), new InetSocketAddress("192.168.1.2", 8081));

            when(mockEntry1.value()).thenReturn(node1.serialize());
            when(mockEntry2.value()).thenReturn(node2.serialize());
            when(mockSearchResponse.entries()).thenReturn(List.of(mockEntry1, mockEntry2));
            when(mockClient.search(anyString())).thenReturn(mockSearchResponse);

            List<NodeConfig> result = discoveryService.getMembers();

            assertNotNull(result);
            assertEquals(2, result.size());
            assertEquals("node1", result.get(0).name());
            assertEquals("node2", result.get(1).name());
        }

        @Test
        @DisplayName("getMembers should return empty list when no members")
        void getMembers_shouldReturnEmptyListWhenNoMembers() throws Exception {
            when(mockSearchResponse.entries()).thenReturn(Collections.emptyList());
            when(mockClient.search(anyString())).thenReturn(mockSearchResponse);

            List<NodeConfig> result = discoveryService.getMembers();

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("getMembers should use instance cluster token")
        void getMembers_shouldUseInstanceClusterToken() throws Exception {
            when(mockSearchResponse.entries()).thenReturn(Collections.emptyList());
            when(mockClient.search(anyString())).thenReturn(mockSearchResponse);

            discoveryService.getMembers();

            verify(mockClient).search("cluster/" + CLUSTER_TOKEN + "/");
        }

        @Test
        @DisplayName("getMembers should handle IOException")
        void getMembers_shouldHandleIOException() throws Exception {
            when(mockClient.search(anyString())).thenThrow(new IOException("Connection failed"));

            assertThrows(IOException.class, () ->
                    discoveryService.getMembers()
            );
        }

        @Test
        @DisplayName("getMembers should handle InterruptedException")
        void getMembers_shouldHandleInterruptedException() throws Exception {
            when(mockClient.search(anyString())).thenThrow(new InterruptedException());

            assertThrows(InterruptedException.class, () ->
                    discoveryService.getMembers()
            );
        }

        @Test
        @DisplayName("getMembers should handle multiple members correctly")
        void getMembers_shouldHandleMultipleMembers() throws Exception {
            NodeConfig node1 = new NodeConfig("node1", new InetSocketAddress("192.168.1.1", 9999), new InetSocketAddress("192.168.1.1", 8080));
            NodeConfig node2 = new NodeConfig("node2", new InetSocketAddress("192.168.1.2", 9999), new InetSocketAddress("192.168.1.2", 8081));
            NodeConfig node3 = new NodeConfig("node3", new InetSocketAddress("192.168.1.3", 9999), new InetSocketAddress("192.168.1.3", 8082));

            SearchResponse.Entry entry3 = mock(SearchResponse.Entry.class);
            when(mockEntry1.value()).thenReturn(node1.serialize());
            when(mockEntry2.value()).thenReturn(node2.serialize());
            when(entry3.value()).thenReturn(node3.serialize());
            when(mockSearchResponse.entries()).thenReturn(List.of(mockEntry1, mockEntry2, entry3));
            when(mockClient.search(anyString())).thenReturn(mockSearchResponse);

            List<NodeConfig> result = discoveryService.getMembers();

            assertEquals(3, result.size());
            assertTrue(result.stream().anyMatch(n -> n.name().equals("node1")));
            assertTrue(result.stream().anyMatch(n -> n.name().equals("node2")));
            assertTrue(result.stream().anyMatch(n -> n.name().equals("node3")));
        }
    }

    // ==================== CONSTRUCTOR TESTS ====================

    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {

        @Test
        @DisplayName("Constructor should store cluster token")
        void constructor_shouldStoreClusterToken() throws Exception {
            UUID customToken = UUID.randomUUID();
            DiscoveryService service = new DiscoveryService(
                    customToken, new InetSocketAddress(kvServer.getHost(), kvServer.getPort())
            );
            injectMockClient(service, mockClient);

            when(mockSearchResponse.entries()).thenReturn(Collections.emptyList());
            when(mockClient.search(anyString())).thenReturn(mockSearchResponse);

            service.getMembers();

            verify(mockClient).search("cluster/" + customToken + "/");
        }

        @Test
        @DisplayName("Constructor should initialize with null client")
        void constructor_shouldInitializeWithNullClient() throws Exception {
            DiscoveryService service = new DiscoveryService(
                    CLUSTER_TOKEN, new InetSocketAddress(kvServer.getHost(), kvServer.getPort()));

            Field clientField = DiscoveryService.class.getDeclaredField("client");
            clientField.setAccessible(true);
            Object client = clientField.get(service);

            assertNull(client, "Client should be null after construction");
        }
    }

    // ==================== INTEGRATION TESTS ====================

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Full workflow: add, get, and delete members")
        void fullWorkflow_shouldWorkCorrectly() throws Exception {
            NodeConfig node1 =
                    new NodeConfig(
                            "node1",
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 8080));
            NodeConfig node2 =
                    new NodeConfig(
                            "node2",
                            new InetSocketAddress("192.168.1.2", 9999),
                            new InetSocketAddress("192.168.1.2", 8081));

            when(mockEntry1.value()).thenReturn(node1.serialize());
            when(mockEntry2.value()).thenReturn(node2.serialize());
            when(mockSearchResponse.entries()).thenReturn(List.of(mockEntry1, mockEntry2));
            when(mockClient.search(anyString())).thenReturn(mockSearchResponse);
            when(mockGetResponse.value()).thenReturn(node1.serialize());
            when(mockClient.get(anyString())).thenReturn(mockGetResponse);

            // Add members
            discoveryService.addMember(CLUSTER_TOKEN, node1);
            discoveryService.addMember(CLUSTER_TOKEN, node2);

            // Get members
            List<NodeConfig> members = discoveryService.getMembers();
            assertEquals(2, members.size());

            // Get specific member
            NodeConfig retrieved = discoveryService.getMember(CLUSTER_TOKEN, "node1");
            assertEquals("node1", retrieved.name());

            // Delete member
            discoveryService.deleteMember(CLUSTER_TOKEN, "node1");

            // Verify all operations
            verify(mockClient, times(2)).put(anyString(), any(byte[].class));
            verify(mockClient, times(1)).delete(anyString());
            verify(mockClient, times(1)).get(anyString());
            verify(mockClient, times(1)).search(anyString());
        }
    }

    // ==================== EDGE CASE TESTS ====================

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle localhost serverAddress")
        void shouldHandleLocalhostAddress() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "localhost-node",
                            new InetSocketAddress("localhost", 9999),
                            new InetSocketAddress("localhost", 8080));

            discoveryService.addMember(CLUSTER_TOKEN, nodeConfig);

            verify(mockClient).put(anyString(), any(byte[].class));
        }

        @Test
        @DisplayName("Should handle IPv6 serverAddress")
        void shouldHandleIPv6Address() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "ipv6-node",
                            new InetSocketAddress("::1", 9999),
                            new InetSocketAddress("::1", 8080));

            discoveryService.addMember(CLUSTER_TOKEN, nodeConfig);

            verify(mockClient).put(anyString(), any(byte[].class));
        }

        @Test
        @DisplayName("Should handle high port number")
        void shouldHandleHighPortNumber() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "high-port-node",
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 65535));

            discoveryService.addMember(CLUSTER_TOKEN, nodeConfig);

            verify(mockClient).put(anyString(), any(byte[].class));
        }

        @Test
        @DisplayName("Should handle long member name")
        void shouldHandleLongMemberName() throws Exception {
            StringBuilder longName = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                longName.append("node");
            }
            NodeConfig nodeConfig =
                    new NodeConfig(
                            longName.toString(),
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 8080));

            discoveryService.addMember(CLUSTER_TOKEN, nodeConfig);

            verify(mockClient).put(contains(longName.toString()), any(byte[].class));
        }

        @Test
        @DisplayName("Should handle unicode member name")
        void shouldHandleUnicodeMemberName() throws Exception {
            NodeConfig nodeConfig =
                    new NodeConfig(
                            "节点一",
                            new InetSocketAddress("192.168.1.1", 9999),
                            new InetSocketAddress("192.168.1.1", 8080));

            discoveryService.addMember(CLUSTER_TOKEN, nodeConfig);

            verify(mockClient).put(contains("节点一"), any(byte[].class));
        }

        @Test
        @DisplayName("Should handle member name with slashes")
        void shouldHandleMemberNameWithSlashes() throws Exception {
            String url = DiscoveryService.getClusterMemberUrl(CLUSTER_TOKEN, "member/with/slashes");
            assertTrue(url.contains("member/with/slashes"));
        }
    }
}