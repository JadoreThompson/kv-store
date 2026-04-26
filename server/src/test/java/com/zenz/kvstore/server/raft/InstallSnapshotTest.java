package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.CommandLogger;
import com.zenz.kvstore.server.logging.Deserializer;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.*;
import com.zenz.kvstore.server.snapshot.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Slf4j
public class InstallSnapshotTest {

    private Server server;

    @Mock
    private StateObject mockStateObject;

    @Mock
    private Manager mockManager;

    @Mock
    private KVStore mockKvstore;

    @Spy
    private RaftLogHandler spyLogHandler;

    @Mock
    private CommandLogger mockCommandLogger;

    private KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter;
    private Path snapshotDir;
    private Path logsDir;

    @BeforeEach
    public void setup() throws Exception {
        server = new Server(new InetSocketAddress("localhost", 9999));
        snapshotter = new KVStoreSnapshotter<>(
                RaftSnapshotHeader.class,
                RaftSnapshotBody.class,
                RaftSnapshotFooter.class);
        snapshotDir = Files.createTempDirectory("test-snapshots");
        snapshotter.setDir(snapshotDir);
        logsDir = Files.createTempDirectory("test-logs");
    }

    @AfterEach
    public void tearDown() throws Exception {
        snapshotDir.toFile().delete();
        logsDir.toFile().delete();
    }

    @Nested
    class Follower {
        @Test
        public void test_installSnapshot_createsFileAndLoadsData() throws Exception {
            final List<RaftLogEntry> entries = createLogEntries(0, 10, 1);

            final Path snapshotPath = snapshotter.snapshot(entries);
            assertTrue(Files.exists(snapshotPath), "Snapshot file should be created");

            final byte[] snapshotData = Files.readAllBytes(snapshotPath);
            final int chunkSize = 100;
            final int totalSize = snapshotData.length;
            int offset = 0;
            int chunkIndex = 0;

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            doReturn(mockKvstore).when(mockManager).getKvstore();
            when(mockStateObject.getCurrentTerm()).thenReturn(1L);
            when(spyLogHandler.getSnapshotter()).thenReturn(snapshotter);
            when(spyLogHandler.getLogger()).thenReturn(mockCommandLogger);
            when(mockCommandLogger.getPath()).thenReturn(logsDir.resolve("test.log"));
            when(mockCommandLogger.loadLogs(any(Path.class), any(Deserializer.class))).thenReturn(Collections.emptyList());
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            InstallSnapshotResponse lastResponse = null;
            while (offset < totalSize) {
                final int remaining = totalSize - offset;
                final int currentChunkSize = Math.min(chunkSize, remaining);
                final byte[] chunk = new byte[currentChunkSize];
                System.arraycopy(snapshotData, offset, chunk, 0, currentChunkSize);
                final boolean isLastChunk = (offset + currentChunkSize) >= totalSize;

                final InstallSnapshot installSnapshot = new InstallSnapshot(
                        "leader",
                        1L,
                        entries.getLast().id,
                        entries.getLast().term,
                        offset,
                        chunk,
                        isLastChunk);

                log.info("Sending install snapshot: {}", installSnapshot);
                final Message response = server.handleInstallSnapshot(installSnapshot);
                assertInstanceOf(InstallSnapshotResponse.class, response);

                lastResponse = (InstallSnapshotResponse) response;
                offset += currentChunkSize;
                chunkIndex++;
            }

            assertNotNull(lastResponse, "Should receive at least one response");
            assertEquals(1L, lastResponse.term(), "Response term should match");

            final Snapshot<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> loadedSnapshot =
                    snapshotter.getSnapshot(snapshotPath);

            assertNotNull(loadedSnapshot, "Loaded snapshot should not be null");
            assertEquals(entries.getLast().id, loadedSnapshot.header().getLastLogId(),
                    "Last log ID should match");
            assertEquals(entries.getLast().term, loadedSnapshot.header().getLastLogTerm(),
                    "Last log term should match");

            Files.deleteIfExists(snapshotPath);
        }

        @Test
        public void test_installSnapshot_leaderSwitch_restartsPersist() throws Exception {
            final List<RaftLogEntry> firstLeaderEntries = createLogEntries(0, 10, 1);
            final List<RaftLogEntry> secondLeaderEntries = createLogEntries(0, 15, 2);

            final Path firstSnapshotPath = snapshotter.snapshot(firstLeaderEntries);
            final Path secondSnapshotPath = snapshotter.snapshot(secondLeaderEntries);

            assertTrue(Files.exists(firstSnapshotPath), "First snapshot file should be created");
            assertTrue(Files.exists(secondSnapshotPath), "Second snapshot file should be created");

            final byte[] firstSnapshotData = Files.readAllBytes(firstSnapshotPath);
            final byte[] secondSnapshotData = Files.readAllBytes(secondSnapshotPath);
            final int chunkSize = 100;

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            doReturn(mockKvstore).when(mockManager).getKvstore();
            when(mockStateObject.getCurrentTerm()).thenReturn(1L);
            when(spyLogHandler.getSnapshotter()).thenReturn(snapshotter);
            when(spyLogHandler.getLogger()).thenReturn(mockCommandLogger);
            when(mockCommandLogger.getPath()).thenReturn(logsDir.resolve("test.log"));
            when(mockCommandLogger.loadLogs(any(Path.class), any(Deserializer.class))).thenReturn(Collections.emptyList());
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            int firstOffset = 0;
            while (firstOffset < firstSnapshotData.length / 2) {
                final int remaining = (firstSnapshotData.length / 2) - firstOffset;
                final int currentChunkSize = Math.min(chunkSize, remaining);
                byte[] firstChunk = new byte[currentChunkSize];
                System.arraycopy(firstSnapshotData, firstOffset, firstChunk, 0, currentChunkSize);
                final boolean isFirstDone = (firstOffset + currentChunkSize) >= (firstSnapshotData.length / 2);

                InstallSnapshot firstInstall = new InstallSnapshot(
                        "leader1",
                        1L,
                        firstLeaderEntries.getLast().id,
                        firstLeaderEntries.getLast().term,
                        firstOffset,
                        firstChunk,
                        isFirstDone);

                log.info("Sending chunk {} from first leader, offset: {}, done: {}",
                        firstOffset / chunkSize, firstOffset, isFirstDone);
                Message response = server.handleInstallSnapshot(firstInstall);
                assertInstanceOf(InstallSnapshotResponse.class, response);

                firstOffset += currentChunkSize;
            }

            when(mockStateObject.getCurrentTerm()).thenReturn(2L);

            int secondOffset = 0;
            InstallSnapshotResponse lastResponse = null;
            while (secondOffset < secondSnapshotData.length) {
                final int remaining = secondSnapshotData.length - secondOffset;
                final int currentChunkSize = Math.min(chunkSize, remaining);
                byte[] chunk = new byte[currentChunkSize];
                System.arraycopy(secondSnapshotData, secondOffset, chunk, 0, currentChunkSize);
                final boolean isLastChunk = (secondOffset + currentChunkSize) >= secondSnapshotData.length;

                InstallSnapshot secondInstall = new InstallSnapshot(
                        "leader2",
                        2L,
                        secondLeaderEntries.getLast().id,
                        secondLeaderEntries.getLast().term,
                        secondOffset,
                        chunk,
                        isLastChunk);

                final Message response = server.handleInstallSnapshot(secondInstall);
                assertInstanceOf(InstallSnapshotResponse.class, response);

                lastResponse = (InstallSnapshotResponse) response;
                secondOffset += currentChunkSize;
            }

            assertNotNull(lastResponse, "Should receive response from second leader");
            assertEquals(2L, lastResponse.term(), "Response term should be from second leader");

            final Snapshot<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> loadedSnapshot =
                    snapshotter.getSnapshot(secondSnapshotPath);

            assertNotNull(loadedSnapshot, "Should load second leader's snapshot");
            assertEquals(secondLeaderEntries.getLast().id, loadedSnapshot.header().getLastLogId(),
                    "Last log ID should be from second leader's entries");
            assertEquals(secondLeaderEntries.getLast().term, loadedSnapshot.header().getLastLogTerm(),
                    "Last log term should be from second leader's entries");

            Files.deleteIfExists(firstSnapshotPath);
            Files.deleteIfExists(secondSnapshotPath);
        }

        @Test
        public void test_installSnapshot_preservesExistingSnapshots() throws Exception {
            final List<RaftLogEntry> existingEntries = createLogEntries(0, 5, 1);
            existingEntries.addAll(createLogEntries(5, 10, 2));

            final Path existingSnapshotPath = snapshotter.snapshot(existingEntries);
            assertTrue(Files.exists(existingSnapshotPath), "Existing snapshot should be created");

            final List<RaftLogEntry> newEntries = createLogEntries(0, 15, 3);
            final Path newSnapshotPath = snapshotter.snapshot(newEntries);

            assertTrue(Files.exists(newSnapshotPath), "New snapshot file should be created");

            final byte[] newSnapshotData = Files.readAllBytes(newSnapshotPath);
            final int chunkSize = 100;

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            doReturn(mockKvstore).when(mockManager).getKvstore();
            when(mockStateObject.getCurrentTerm()).thenReturn(3L);
            when(spyLogHandler.getSnapshotter()).thenReturn(snapshotter);
            when(spyLogHandler.getLogger()).thenReturn(mockCommandLogger);
            when(mockCommandLogger.getPath()).thenReturn(logsDir.resolve("test.log"));
            when(mockCommandLogger.loadLogs(any(Path.class), any(Deserializer.class))).thenReturn(Collections.emptyList());
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            int offset = 0;
            InstallSnapshotResponse lastResponse = null;
            while (offset < newSnapshotData.length) {
                final int remaining = newSnapshotData.length - offset;
                final int currentChunkSize = Math.min(chunkSize, remaining);
                byte[] chunk = new byte[currentChunkSize];
                System.arraycopy(newSnapshotData, offset, chunk, 0, currentChunkSize);
                final boolean isLastChunk = (offset + currentChunkSize) >= newSnapshotData.length;

                InstallSnapshot installSnapshot = new InstallSnapshot(
                        "leader",
                        3L,
                        newEntries.getLast().id,
                        newEntries.getLast().term,
                        offset,
                        chunk,
                        isLastChunk);

                Message response = server.handleInstallSnapshot(installSnapshot);
                assertInstanceOf(InstallSnapshotResponse.class, response);

                lastResponse = (InstallSnapshotResponse) response;
                offset += currentChunkSize;
            }

            assertNotNull(lastResponse, "Should receive response");
            assertEquals(3L, lastResponse.term(), "Response term should match");

            assertTrue(Files.exists(existingSnapshotPath), "Existing snapshot should NOT be deleted");
            assertTrue(Files.exists(newSnapshotPath), "New snapshot should be created");

            Files.deleteIfExists(existingSnapshotPath);
            Files.deleteIfExists(newSnapshotPath);
        }

        @Test
        public void test_installSnapshot_restoresKvStoreAndUpdatesLogHandler() throws Exception {
            final List<RaftLogEntry> entries = createLogEntries(0, 10, 5);

            final Path snapshotPath = snapshotter.snapshot(entries);
            assertTrue(Files.exists(snapshotPath), "Snapshot file should be created");

            final byte[] snapshotData = Files.readAllBytes(snapshotPath);
            final int chunkSize = 100;
            final long lastIncludedId = entries.getLast().id;
            final long lastIncludedTerm = entries.getLast().term;

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            doReturn(mockKvstore).when(mockManager).getKvstore();
            when(mockStateObject.getCurrentTerm()).thenReturn(5L);
            when(spyLogHandler.getSnapshotter()).thenReturn(snapshotter);
            when(spyLogHandler.getLogger()).thenReturn(mockCommandLogger);
            when(mockCommandLogger.getPath()).thenReturn(logsDir.resolve("test.log"));
            when(mockCommandLogger.loadLogs(any(Path.class), any(Deserializer.class))).thenReturn(Collections.emptyList());
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            int offset = 0;
            InstallSnapshotResponse lastResponse = null;
            while (offset < snapshotData.length) {
                final int remaining = snapshotData.length - offset;
                final int currentChunkSize = Math.min(chunkSize, remaining);
                byte[] chunk = new byte[currentChunkSize];
                System.arraycopy(snapshotData, offset, chunk, 0, currentChunkSize);
                final boolean isLastChunk = (offset + currentChunkSize) >= snapshotData.length;

                InstallSnapshot installSnapshot = new InstallSnapshot(
                        "leader",
                        5L,
                        lastIncludedId,
                        lastIncludedTerm,
                        offset,
                        chunk,
                        isLastChunk);

                log.info("Sending install snapshot: {}", installSnapshot);
                Message response = server.handleInstallSnapshot(installSnapshot);
                assertInstanceOf(InstallSnapshotResponse.class, response);

                lastResponse = (InstallSnapshotResponse) response;
                offset += currentChunkSize;
            }

            assertNotNull(lastResponse, "Should receive response");

            ArgumentCaptor<KVStore> captor = ArgumentCaptor.forClass(KVStore.class);
            verify(mockManager).setKvstore(captor.capture());
            final KVStore newKvstore = captor.getValue();
            final RaftLogHandler newLogHandler = (RaftLogHandler) newKvstore.getLogHandler();
            assertEquals(lastIncludedId, newLogHandler.getLogId(),
                    "Log handler logId should be lastIncludedId - 1");
            assertEquals(lastIncludedTerm, newLogHandler.getTerm(),
                    "Log handler term should be lastIncludedTerm");

            final Snapshot<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> loadedSnapshot =
                    snapshotter.getSnapshot(snapshotPath);

            assertNotNull(loadedSnapshot, "Snapshot should be loadable");
            assertEquals(lastIncludedId, loadedSnapshot.header().getLastLogId(),
                    "Snapshot header last log ID should match");
            assertEquals(lastIncludedTerm, loadedSnapshot.header().getLastLogTerm(),
                    "Snapshot header last log term should match");

            Files.deleteIfExists(snapshotPath);
        }

        @Test
        public void test_installSnapshot_multipleSnapshots_containsLastEntries() throws Exception {
            final List<RaftLogEntry> firstSnapshotEntries = createLogEntries(0, 5, 1);
            final List<RaftLogEntry> secondSnapshotEntries = createLogEntries(5, 15, 2);

            final Path firstPath = snapshotter.snapshot(firstSnapshotEntries);
            final Path secondPath = snapshotter.snapshot(secondSnapshotEntries);
            assertTrue(Files.exists(firstPath), "First snapshot should exist");
            assertTrue(Files.exists(secondPath), "Second snapshot should exist");

            final byte[] firstData = Files.readAllBytes(firstPath);
            final byte[] secondData = Files.readAllBytes(secondPath);
            final int chunkSize = 100;

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "follower",
                    new InetSocketAddress("localhost", 9999)));
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            doReturn(mockKvstore).when(mockManager).getKvstore();
            when(mockStateObject.getCurrentTerm()).thenReturn(1L);
            when(spyLogHandler.getSnapshotter()).thenReturn(snapshotter);
            when(spyLogHandler.getLogger()).thenReturn(mockCommandLogger);
            when(mockCommandLogger.getPath()).thenReturn(logsDir.resolve("test.log"));
            when(mockCommandLogger.loadLogs(any(Path.class), any(Deserializer.class))).thenReturn(Collections.emptyList());
            server.setManager(mockManager);
            server.setStateObject(mockStateObject);

            int offset = 0;
            InstallSnapshotResponse lastResponse = null;
            while (offset < firstData.length) {
                final int remaining = firstData.length - offset;
                final int currentChunkSize = Math.min(chunkSize, remaining);
                byte[] chunk = new byte[currentChunkSize];
                System.arraycopy(firstData, offset, chunk, 0, currentChunkSize);
                final boolean isLastChunk = (offset + currentChunkSize) >= firstData.length;

                InstallSnapshot installSnapshot = new InstallSnapshot(
                        "leader1",
                        1L,
                        firstSnapshotEntries.getLast().id,
                        firstSnapshotEntries.getLast().term,
                        offset,
                        chunk,
                        isLastChunk);

                Message response = server.handleInstallSnapshot(installSnapshot);
                assertInstanceOf(InstallSnapshotResponse.class, response);

                lastResponse = (InstallSnapshotResponse) response;
                offset += currentChunkSize;
            }

            assertNotNull(lastResponse, "Should receive response from first snapshot");

            when(mockStateObject.getCurrentTerm()).thenReturn(2L);

            offset = 0;
            while (offset < secondData.length) {
                final int remaining = secondData.length - offset;
                final int currentChunkSize = Math.min(chunkSize, remaining);
                byte[] chunk = new byte[currentChunkSize];
                System.arraycopy(secondData, offset, chunk, 0, currentChunkSize);
                final boolean isLastChunk = (offset + currentChunkSize) >= secondData.length;

                InstallSnapshot installSnapshot = new InstallSnapshot(
                        "leader2",
                        2L,
                        secondSnapshotEntries.getLast().id,
                        secondSnapshotEntries.getLast().term,
                        offset,
                        chunk,
                        isLastChunk);

                Message response = server.handleInstallSnapshot(installSnapshot);
                assertInstanceOf(InstallSnapshotResponse.class, response);

                lastResponse = (InstallSnapshotResponse) response;
                offset += currentChunkSize;
            }

            assertNotNull(lastResponse, "Should receive response from second snapshot");

            ArgumentCaptor<KVStore> kvStoreCaptor = ArgumentCaptor.forClass(KVStore.class);
            verify(mockManager, atLeastOnce()).setKvstore(kvStoreCaptor.capture());
            final KVStore capturedKvStore = kvStoreCaptor.getAllValues().getLast();
            final RaftLogHandler capturedLogHandler = (RaftLogHandler) capturedKvStore.getLogHandler();

            final long expectedLogId = secondSnapshotEntries.getLast().id;
            final long expectedTerm = secondSnapshotEntries.getLast().term;

            assertEquals(expectedLogId, capturedLogHandler.getLogId(),
                    "Log handler logId should reflect the last snapshot's lastIncludedId - 1");
            assertEquals(expectedTerm, capturedLogHandler.getTerm(),
                    "Log handler term should reflect the last snapshot's lastIncludedTerm");

            assertTrue(capturedLogHandler.getEntries().isEmpty(),
                    "Log handler entries should be empty after installing snapshot");

            final Snapshot<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> loadedSnapshot =
                    snapshotter.getSnapshot(secondPath);

            assertNotNull(loadedSnapshot, "Should load the second snapshot");
            assertEquals(secondSnapshotEntries.getLast().id, loadedSnapshot.header().getLastLogId(),
                    "Second snapshot should have its own last log ID");
            assertEquals(secondSnapshotEntries.getLast().term, loadedSnapshot.header().getLastLogTerm(),
                    "Second snapshot should have its own last log term");

            assertTrue(loadedSnapshot.header().getLastLogId() > firstSnapshotEntries.getLast().id,
                    "Second snapshot should have more entries than first");

            Files.deleteIfExists(firstPath);
            Files.deleteIfExists(secondPath);
        }
    }

    @Nested
    class Leader {
        private Client client;

        @BeforeEach
        public void setup() throws IOException {
            client = new Client(new InetSocketAddress("localhost", 9999));
        }

        @Test
        public void test_installSnapshotResponse_higherTerm_stepsDown() throws IOException {
            final List<RaftLogEntry> entries = createLogEntries(0, 10, 1);
            final var snapshotter = spyLogHandler.getSnapshotter();
            snapshotter.setDir(snapshotDir);
            snapshotter.snapshot(entries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "leader",
                    new InetSocketAddress("localhost", 9999)));
            when(mockManager.getKvstore()).thenReturn(mockKvstore);
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();

            client.setStateObject(mockStateObject);
            client.setManager(mockManager);

            client.handleAppendEntryResponse(new AppendEntryResponse(
                    0L,
                    AppendEntryResponse.FailureReason.PREV_LOG_MISMATCH,
                    3L,
                    1L,
                    -1L,
                    -1L
            ));
            when(mockStateObject.getCurrentTerm()).thenReturn(4L);
            final InstallSnapshotResponse response = new InstallSnapshotResponse(5L);
            final Message result = client.handleInstallSnapshotResponse(response);

            assertNull(result, "Leader should step down when receiving higher term");
            verify(mockStateObject, times(1)).setState(State.FOLLOWER);
        }

        @Test
        public void test_finalInstallSnapshotResponse_returnsAppendEntry() throws IOException {
            final List<RaftLogEntry> entries = createLogEntries(0, 5, 1);
            final var snapshotter = spyLogHandler.getSnapshotter();
            snapshotter.setDir(snapshotDir);
            snapshotter.snapshot(entries);

            when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                    "leader",
                    new InetSocketAddress("localhost", 9999)));
            when(mockManager.getKvstore()).thenReturn(mockKvstore);
            doReturn(spyLogHandler).when(mockKvstore).getLogHandler();
            when(mockStateObject.getCurrentTerm()).thenReturn(1L);

            client.setStateObject(mockStateObject);
            client.setManager(mockManager);

            client.handleAppendEntryResponse(new AppendEntryResponse(
                    0L,
                    AppendEntryResponse.FailureReason.PREV_LOG_MISMATCH,
                    3L,
                    1L,
                    -1L,
                    -1L
            ));
            final InstallSnapshotResponse response = new InstallSnapshotResponse(1L);
            final Message result = client.handleInstallSnapshotResponse(response);

            assertNotNull(result, "Should send AppendEntry after successful snapshot install");
            assertInstanceOf(AppendEntry.class, result);
        }
    }

    private List<RaftLogEntry> createLogEntries(final int startId, final int endId, final long term) {
        final List<RaftLogEntry> entries = new ArrayList<>();
        for (int i = startId; i <= endId; i++) {
            entries.add(new RaftLogEntry(
                    i,
                    term,
                    new PutCommand("key-" + i, ("value-" + i).getBytes(StandardCharsets.UTF_8))
            ));
        }
        return entries;
    }
}