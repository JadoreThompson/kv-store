package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.AppendEntry;
import com.zenz.kvstore.server.raft.message.AppendEntryResponse;
import com.zenz.kvstore.server.raft.message.InstallSnapshot;
import com.zenz.kvstore.server.raft.message.Message;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Slf4j
public class AppendEntryTest {

    private Server server;

    @Mock
    private StateObject mockStateObject;

    @Mock
    private Manager mockManager;

    @Mock
    private KVStore mockKvstore;

    @Spy
    private RaftLogHandler spyLogHandler;

    @BeforeEach
    public void setup() {
        server = new Server(new InetSocketAddress("localhost", 9999));
    }

    @Nested
    class Follower {
        @Test
        public void testSamePrevLogId_andPrevLogTerm() {
            final List<RaftLogEntry> entries = createLogEntries(1, 10, 1);
            final AppendEntry appendEntry = new AppendEntry(
                    "leader",
                    1L,
                    0L,
                    0L,
                    entries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            when(mockStateObject.getCurrentTerm()).thenReturn(1L);
            when(mockStateObject.getLeaderId()).thenReturn(null);
            when(spyLogHandler.getSeedEntry()).thenReturn(new RaftLogEntry(
                    0L,
                    0L,
                    new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8))));
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);
            when(mockStateObject.getLogHandler()).thenReturn(spyLogHandler);

            final Message response = server.handleAppendEntry(appendEntry);
            assertInstanceOf(AppendEntryResponse.class, response);

            final AppendEntryResponse appendEntryResponse = (AppendEntryResponse) response;
            log.info("Append entry response: {}", appendEntryResponse);
            assertTrue(appendEntryResponse.isSuccess());
            assertEquals(entries.getLast().id, appendEntryResponse.lastLogId());
            assertEquals(entries.getLast().term, appendEntryResponse.lastLogTerm());
            assertEquals(0L, appendEntryResponse.prevLogId());
            assertEquals(0L, appendEntryResponse.prevLogTerm());
        }

        @Test
        public void test_staleTerm_rejectsAppendEntry() {
            final List<RaftLogEntry> entries = List.of(new RaftLogEntry(
                    1L,
                    1L,
                    new PutCommand("key", "value".getBytes(StandardCharsets.UTF_8))));
            final AppendEntry appendEntry = new AppendEntry(
                    "leader",
                    1L,
                    0L,
                    0L,
                    entries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            when(mockStateObject.getCurrentTerm()).thenReturn(2L);
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            final Message response = server.handleAppendEntry(appendEntry);
            assertInstanceOf(AppendEntryResponse.class, response);

            final AppendEntryResponse appendEntryResponse = (AppendEntryResponse) response;
            assertFalse(appendEntryResponse.isSuccess());
            assertEquals(
                    AppendEntryResponse.FailureReason.GREATER_TERM,
                    appendEntryResponse.failureReason());
        }

        @Test
        public void test_prevLogMismatch_rejectsAppendEntry() {
            final List<RaftLogEntry> entries = List.of(new RaftLogEntry(
                    2L,
                    1L,
                    new PutCommand("key", "value".getBytes(StandardCharsets.UTF_8))));
            final AppendEntry appendEntry = new AppendEntry(
                    "leader",
                    2L,
                    1L,
                    1L,
                    entries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            doReturn(mockKvstore).when(mockManager).getKvstore();
            when(mockStateObject.getCurrentTerm()).thenReturn(2L);
            when(mockStateObject.getLeaderId()).thenReturn("leader");
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            final Message response = server.handleAppendEntry(appendEntry);
            assertInstanceOf(AppendEntryResponse.class, response);

            final AppendEntryResponse appendEntryResponse = (AppendEntryResponse) response;
            assertFalse(appendEntryResponse.isSuccess());
            assertEquals(
                    AppendEntryResponse.FailureReason.PREV_LOG_MISMATCH,
                    appendEntryResponse.failureReason());
        }

        @Test
        public void test_emptyEntries_heartbeat_succeeds() {
            final AppendEntry appendEntry = new AppendEntry(
                    "leader",
                    1L,
                    0L,
                    0L,
                    new ArrayList<>());

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            when(mockStateObject.getCurrentTerm()).thenReturn(1L);
            when(mockStateObject.getLeaderId()).thenReturn(null);
            when(spyLogHandler.getSeedEntry()).thenReturn(new RaftLogEntry(
                    0L,
                    0L,
                    new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8))));
            when(spyLogHandler.getLogId()).thenReturn(0L);
            when(spyLogHandler.getTerm()).thenReturn(0L);
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);
            when(mockStateObject.getLogHandler()).thenReturn(spyLogHandler);

            final Message response = server.handleAppendEntry(appendEntry);
            assertInstanceOf(AppendEntryResponse.class, response);

            final AppendEntryResponse appendEntryResponse = (AppendEntryResponse) response;
            assertTrue(appendEntryResponse.isSuccess());
            assertNull(appendEntryResponse.failureReason());
        }

        @Test
        public void test_sameTermAsFollower_acceptsAppendEntry() {
            final List<RaftLogEntry> entries = List.of(new RaftLogEntry(
                    1L,
                    1L,
                    new PutCommand("key", "value".getBytes(StandardCharsets.UTF_8))));
            final AppendEntry appendEntry = new AppendEntry(
                    "leader",
                    1L,
                    0L,
                    0L,
                    entries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            when(mockStateObject.getCurrentTerm()).thenReturn(1L);
            when(mockStateObject.getLeaderId()).thenReturn(null);
            when(spyLogHandler.getSeedEntry()).thenReturn(new RaftLogEntry(
                    0L,
                    0L,
                    new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8))));
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);
            when(mockStateObject.getLogHandler()).thenReturn(spyLogHandler);

            final Message response = server.handleAppendEntry(appendEntry);
            assertInstanceOf(AppendEntryResponse.class, response);

            final AppendEntryResponse appendEntryResponse = (AppendEntryResponse) response;
            assertTrue(appendEntryResponse.isSuccess());
        }

        @Test
        @DisplayName("Remote log overrides conflicting local entries")
        public void test_logEntryWithDifferentTerm() {
            final List<RaftLogEntry> localEntries = createLogEntries(0, 5, 1);
            final List<RaftLogEntry> leaderEntries = createLogEntries(0, 2, 1);
            leaderEntries.addAll(createLogEntries(3, 7, 2));

            final AppendEntry appendEntry = new AppendEntry(
                    "leader",
                    2L,
                    0L,
                    0L,
                    leaderEntries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            doReturn(mockKvstore).when(mockManager).getKvstore();
            when(mockStateObject.getCurrentTerm()).thenReturn(2L);
            when(spyLogHandler.getSeedEntry()).thenReturn(new RaftLogEntry(
                    0L,
                    0L,
                    new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8))));
            when(spyLogHandler.getEntries()).thenReturn(localEntries);
            spyLogHandler.setLogId(2L);
            spyLogHandler.setTerm(1L);
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            final Message response = server.handleAppendEntry(appendEntry);
            assertInstanceOf(AppendEntryResponse.class, response);

            final AppendEntryResponse appendEntryResponse = (AppendEntryResponse) response;
            assertTrue(appendEntryResponse.isSuccess(), "Append entry should succeed after truncating conflicting entries");
            assertEquals(7L, appendEntryResponse.lastLogId(), "Last log ID should match the leader's last entry");
            assertEquals(2L, appendEntryResponse.lastLogTerm(), "Last log term should match the leader's term");
        }
    }

    @Nested
    class Leader {

        private Client client;

        private Path snapshotDir;

        @BeforeEach
        public void setup() throws IOException {
            client = new Client(new InetSocketAddress("localhost", 9999));
            snapshotDir = Files.createTempDirectory("temp-snapshots-");
        }

        @AfterEach
        public void teardown() throws IOException {
            snapshotDir.toFile().delete();
        }

        @Test
        public void test_highTermResponse_setsStateToFollower() throws IOException {
            final AppendEntryResponse appendEntryResponse = new AppendEntryResponse(
                    2L,
                    AppendEntryResponse.FailureReason.GREATER_TERM,
                    0L,
                    0L,
                    1L,
                    1L);

            client.setStateObject(mockStateObject);
            final Message response = client.handleAppendEntryResponse(appendEntryResponse);

            assertNull(response, "There is no output suitable for this input.");
            verify(mockStateObject, times(1)).setState(State.FOLLOWER);
        }

        @Test
        public void test_prevLogMismatch_sendsInstallSnapshot() throws IOException {
            final List<RaftLogEntry> entries = createLogEntries(0, 5, 1);
            final var snapshotter = spyLogHandler.getSnapshotter();
            snapshotter.setDir(snapshotDir);
            snapshotter.snapshot(entries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "leader",
                    new InetSocketAddress("localhost", 9999)));
            when(mockStateObject.getLogHandler()).thenReturn(spyLogHandler);

            client.setStateObject(mockStateObject);
            client.setManager(mockManager);

            final AppendEntryResponse appendEntryResponse = new AppendEntryResponse(
                    2L,
                    AppendEntryResponse.FailureReason.PREV_LOG_MISMATCH,
                    0L,
                    0L,
                    -1,
                    -1);
            final Message response = client.handleAppendEntryResponse(appendEntryResponse);

            assertNotNull(response);
            assertInstanceOf(InstallSnapshot.class, response, "Install snapshot should have been returned.");

            final InstallSnapshot installSnapshot = (InstallSnapshot) response;
            assertEquals(entries.getLast().getId(), installSnapshot.lastIncludedId());
            assertEquals(entries.getLast().getTerm(), installSnapshot.lastIncludedTerm());
        }

        @Test
        public void test_successResponse_returnsNextBatch_of_logEntries() throws IOException {
            final List<RaftLogEntry> entries = createLogEntries(0, 20, 1);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "leader",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(entries).when(spyLogHandler).getEntries();
            when(mockStateObject.getLogHandler()).thenReturn(spyLogHandler);

            client.setStateObject(mockStateObject);
            client.setManager(mockManager);
            client.setBatchSize(10);
            final AppendEntry initialAppendEntry = client.createAppendEntryRequest();

            final AppendEntryResponse successResponse = new AppendEntryResponse(
                    1L,
                    null,
                    5L,
                    1L,
                    10L,
                    1L);
            final Message response = client.handleAppendEntryResponse(successResponse);

            assertNotNull(response, "Success response should trigger next AppendEntry request");
            assertInstanceOf(AppendEntry.class, response);

            final AppendEntry nextRequest = (AppendEntry) response;
            assertEquals(10L, nextRequest.prevLogId(), "prevLogId should match the last committed log ID");
            assertEquals(1L, nextRequest.prevLogTerm(), "prevLogTerm should match");
            assertEquals(
                    entries.getLast().getId(),
                    nextRequest.entries().getLast().getId(),
                    "The next request should contain the next batch of log entries");
        }

        @Test
        public void test_higherTermResponse_stepsDown() throws IOException {
            final AppendEntryResponse higherTermResponse = new AppendEntryResponse(
                    5L,
                    AppendEntryResponse.FailureReason.GREATER_TERM,
                    0L,
                    0L,
                    10L,
                    3L);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "leader",
                    new InetSocketAddress("localhost", 9999)));

            client.setStateObject(mockStateObject);
            client.setManager(mockManager);

            final Message response = client.handleAppendEntryResponse(higherTermResponse);

            assertNull(response, "Leader should step down on higher term");
            verify(mockStateObject, times(1)).setState(State.FOLLOWER);
        }

        @Test
        public void test_heartbeat_emptyEntries() throws IOException {
            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "leader",
                    new InetSocketAddress("localhost", 9999)));
            when(spyLogHandler.getSeedEntry()).thenReturn(new RaftLogEntry(0L, 0L, new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8))));
            when(spyLogHandler.getEntries()).thenReturn(new ArrayList<>());
            when(mockStateObject.getLogHandler()).thenReturn(spyLogHandler);

            client.setStateObject(mockStateObject);
            client.setManager(mockManager);

            final Message response = client.handleAppendEntryResponse(new AppendEntryResponse(
                    1L,
                    null,
                    0L,
                    0L,
                    10L,
                    1L));

            assertNotNull(response, "Should return AppendEntry for heartbeat");
            assertInstanceOf(AppendEntry.class, response);

            final AppendEntry heartbeat = (AppendEntry) response;
            assertTrue(heartbeat.entries().isEmpty(), "Heartbeat should have empty entries");
        }

        @Test
        public void test_batchEntries_respectsBatchSize() throws IOException {
            final List<RaftLogEntry> manyEntries = createLogEntries(0, 25, 1);
            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "leader",
                    new InetSocketAddress("localhost", 9999)));
            RaftLogEntry seed = new RaftLogEntry(0L, 0L, new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8)));
            when(spyLogHandler.getSeedEntry()).thenReturn(seed);
            when(spyLogHandler.getEntries()).thenReturn(manyEntries);
            when(mockStateObject.getLogHandler()).thenReturn(spyLogHandler);

            spyLogHandler.setLogId(0L);
            spyLogHandler.setTerm(1L);

            client.setStateObject(mockStateObject);
            client.setManager(mockManager);

            final Message request1 = client.handleAppendEntryResponse(new AppendEntryResponse(
                    1L,
                    null,
                    0L,
                    0L,
                    10L,
                    1L));

            assertNotNull(request1);
            assertTrue(((AppendEntry) request1).entries().size() <= 10, "Should respect batch size of 10");
        }
    }

    private List<RaftLogEntry> createLogEntries(final int startId, final int endId, final long term) {
        final List<RaftLogEntry> entries = new ArrayList<>();
        for (int i = startId; i < endId; i++) {
            entries.add(new RaftLogEntry(
                    i + 1,
                    term,
                    new PutCommand("key-" + i, ("value-" + i).getBytes(StandardCharsets.UTF_8))
            ));
        }
        return entries;
    }
}
