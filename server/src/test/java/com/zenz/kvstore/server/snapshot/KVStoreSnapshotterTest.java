package com.zenz.kvstore.server.snapshot;

import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.logging.LogEntry;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KVStoreSnapshotterTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("SingleSnapshot Tests")
    class SingleSnapshotTests {
        private KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter;

        @BeforeEach
        void setUp() {
            snapshotter = new KVStoreSnapshotter<>(
                    SingleSnapshotHeader.class,
                    SingleSnapshotBody.class,
                    SingleSnapshotFooter.class
            );
            snapshotter.setDir(tempDir);
        }

        @Test
        void snapshot_createsFileWithCorrectName() throws Exception {
            List<LogEntry> entries = createLogEntries(5, 10);

            Path snapshotPath = snapshotter.snapshot(entries);

            assertTrue(Files.exists(snapshotPath), "Snapshot file should exist");
        }

        @Test
        void snapshot_fileContainsData() throws Exception {
            List<LogEntry> entries = createLogEntries(3, 5);

            Path snapshotPath = snapshotter.snapshot(entries);

            assertTrue(Files.size(snapshotPath) > 0, "Snapshot file should not be empty");
        }

        @Test
        void getHeader_returnsCorrectValues() throws Exception {
            List<LogEntry> entries = createLogEntries(3, 5);

            Path snapshotPath = snapshotter.snapshot(entries);

            SingleSnapshotHeader header = snapshotter.getHeader(snapshotPath);

            assertEquals(1L, header.getVersion(), "Version should be 1");
            assertEquals(5, header.getFirstLogId(), "First log ID should be 5");
            assertEquals(7, header.getLastLogId(), "Last log ID should be 7");
        }

        @Test
        void getFooter_returnsTimestamp() throws Exception {
            List<LogEntry> entries = createLogEntries(1, 1);

            long beforeSnapshot = System.currentTimeMillis();
            Path snapshotPath = snapshotter.snapshot(entries);
            long afterSnapshot = System.currentTimeMillis();

            SingleSnapshotFooter footer = snapshotter.getFooter(snapshotPath);

            assertTrue(footer.getTimestamp() >= beforeSnapshot,
                    "Timestamp should be >= time before snapshot");
            assertTrue(footer.getTimestamp() <= afterSnapshot,
                    "Timestamp should be <= time after snapshot");
        }

        @Test
        void findSnapshot_returnsPathForExactMatch() throws Exception {
            List<LogEntry> entries = createLogEntries(5, 6);
            snapshotter.snapshot(entries);

            Path foundPath = snapshotter.findSnapshot(10);

            assertNotNull(foundPath, "Should find snapshot with logId <= requested");
        }

        @Test
        void findSnapshot_returnsNullWhenNoMatch() throws Exception {
            List<LogEntry> entries = createLogEntries(5, 10);
            snapshotter.snapshot(entries);

            Path foundPath = snapshotter.findSnapshot(100);

            assertNull(foundPath, "Should return null when no snapshot contains the logId");
        }

        @Test
        void findSnapshot_returnsNullForEmptyDirectory() {
            Path foundPath = snapshotter.findSnapshot(5);

            assertNull(foundPath, "Should return null when directory is empty");
        }

        @Test
        void snapshot_throwsForNullEntries() {
            assertThrows(IllegalArgumentException.class, () -> snapshotter.snapshot(null));
        }

        @Test
        void snapshot_throwsForEmptyEntries() {
            assertThrows(IllegalArgumentException.class, () -> snapshotter.snapshot(new ArrayList<>()));
        }

        @Test
        void getHeader_throwsForNonExistentFile() {
            Path nonExistent = tempDir.resolve("nonexistent.snapshot");

            assertThrows(Exception.class, () -> snapshotter.getHeader(nonExistent));
        }

        @Test
        void snapshot_multipleSnapshotsAllRetrievable() throws Exception {
            List<LogEntry> entries1 = createLogEntries(3, 1);
            Path path1 = snapshotter.snapshot(entries1);

            List<LogEntry> entries2 = createLogEntries(3, 4);
            Path path2 = snapshotter.snapshot(entries2);

            SingleSnapshotHeader header1 = snapshotter.getHeader(path1);
            SingleSnapshotHeader header2 = snapshotter.getHeader(path2);

            assertEquals(3, header1.getLastLogId());
            assertEquals(6, header2.getLastLogId());
        }

        private List<LogEntry> createLogEntries(int count, int startId) {
            List<LogEntry> entries = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                entries.add(new LogEntry(startId + i, new PutCommand("key" + i, ("value" + i).getBytes())));
            }
            return entries;
        }
    }

    @Nested
    @DisplayName("RaftSnapshot Tests")
    class RaftSnapshotTests {
        private KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter;

        @BeforeEach
        void setUp() {
            snapshotter = new KVStoreSnapshotter<>(
                    RaftSnapshotHeader.class,
                    RaftSnapshotBody.class,
                    RaftSnapshotFooter.class
            );
            snapshotter.setDir(tempDir);
        }

        @Test
        void snapshot_createsFileWithCorrectName() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(5, 10, 1);

            Path snapshotPath = snapshotter.snapshot(entries);

            assertTrue(Files.exists(snapshotPath), "Snapshot file should exist");
        }

        @Test
        void snapshot_fileContainsData() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(3, 5, 1);

            Path snapshotPath = snapshotter.snapshot(entries);

            assertTrue(Files.size(snapshotPath) > 0, "Snapshot file should not be empty");
        }

        @Test
        void getHeader_returnsCorrectValues() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(3, 5, 1);

            Path snapshotPath = snapshotter.snapshot(entries);

            RaftSnapshotHeader header = snapshotter.getHeader(snapshotPath);

            assertEquals(1L, header.getVersion(), "Version should be 1");
            assertEquals(5, header.getFirstLogId(), "First log ID should be 5");
            assertEquals(1, header.getFirstLogTerm(), "First log term should be 1");
            assertEquals(7, header.getLastLogId(), "Last log ID should be 7");
            final long lastLogTerm = entries.getLast().term;
            assertEquals(lastLogTerm, header.getLastLogTerm(), String.format("Last log term should be %s", lastLogTerm));
        }

        @Test
        void getHeader_returnsCorrectTermValues() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(3, 5, 3);

            Path snapshotPath = snapshotter.snapshot(entries);

            RaftSnapshotHeader header = snapshotter.getHeader(snapshotPath);

            assertEquals(5, header.getFirstLogId());
            assertEquals(3, header.getFirstLogTerm());
            assertEquals(7, header.getLastLogId());
            assertEquals(entries.getLast().term, header.getLastLogTerm());
        }

        @Test
        void getFooter_returnsTimestamp() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(1, 1, 1);

            long beforeSnapshot = System.currentTimeMillis();
            Path snapshotPath = snapshotter.snapshot(entries);
            long afterSnapshot = System.currentTimeMillis();

            RaftSnapshotFooter footer = snapshotter.getFooter(snapshotPath);

            assertTrue(footer.getTimestamp() >= beforeSnapshot,
                    "Timestamp should be >= time before snapshot");
            assertTrue(footer.getTimestamp() <= afterSnapshot,
                    "Timestamp should be <= time after snapshot");
        }

        @Test
        void getSnapshot_returnsAllComponents() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(2, 5, 2);

            Path snapshotPath = snapshotter.snapshot(entries);

            Snapshot<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshot =
                    snapshotter.getSnapshot(snapshotPath);

            assertNotNull(snapshot.header());
            assertNotNull(snapshot.body());
            assertNotNull(snapshot.footer());

            assertEquals(5, snapshot.header().getFirstLogId());
            assertEquals(2, snapshot.header().getFirstLogTerm());
            assertEquals(6, snapshot.header().getLastLogId());
            assertEquals(entries.getLast().term, snapshot.header().getLastLogTerm());
        }

        @Test
        void findSnapshot_returnsPathForExactMatch() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(5, 6, 1);
            snapshotter.snapshot(entries);

            Path foundPath = snapshotter.findSnapshot(10);

            assertNotNull(foundPath, "Should find snapshot with logId <= requested");
        }

        @Test
        void findSnapshot_returnsPathForHigherLogId() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(5, 6, 1);
            snapshotter.snapshot(entries);

            Path foundPath = snapshotter.findSnapshot(5);

            assertNotNull(foundPath, "Should find snapshot with logId <= requested");
        }

        @Test
        void findSnapshot_returnsFirstMatchingSnapshot() throws Exception {
            List<RaftLogEntry> entries1 = createRaftLogEntries(5, 6, 1);
            snapshotter.snapshot(entries1);

            List<RaftLogEntry> entries2 = createRaftLogEntries(5, 16, 2);
            snapshotter.snapshot(entries2);

            Path foundPath = snapshotter.findSnapshot(12);

            assertNotNull(foundPath);
        }

        @Test
        void findSnapshot_returnsNullWhenNoMatch() throws Exception {
            List<RaftLogEntry> entries = createRaftLogEntries(5, 10, 1);
            snapshotter.snapshot(entries);

            Path foundPath = snapshotter.findSnapshot(100);

            assertNull(foundPath, "Should return null when no snapshot contains the logId");
        }

        @Test
        void findSnapshot_returnsNullForEmptyDirectory() {
            Path foundPath = snapshotter.findSnapshot(5);

            assertNull(foundPath, "Should return null when directory is empty");
        }

        @Test
        void snapshot_throwsForNullEntries() {
            assertThrows(IllegalArgumentException.class, () -> snapshotter.snapshot(null));
        }

        @Test
        void snapshot_throwsForEmptyEntries() {
            assertThrows(IllegalArgumentException.class, () -> snapshotter.snapshot(new ArrayList<>()));
        }

        @Test
        void getHeader_throwsForNonExistentFile() {
            Path nonExistent = tempDir.resolve("nonexistent.snapshot");

            assertThrows(Exception.class, () -> snapshotter.getHeader(nonExistent));
        }

        @Test
        void snapshot_multipleSnapshotsAllRetrievable() throws Exception {
            List<RaftLogEntry> entries1 = createRaftLogEntries(3, 1, 1);
            Path path1 = snapshotter.snapshot(entries1);

            List<RaftLogEntry> entries2 = createRaftLogEntries(3, 4, 2);
            Path path2 = snapshotter.snapshot(entries2);

            RaftSnapshotHeader header1 = snapshotter.getHeader(path1);
            RaftSnapshotHeader header2 = snapshotter.getHeader(path2);

            assertEquals(3, header1.getLastLogId());
            assertEquals(entries1.getLast().term, header1.getLastLogTerm());
            assertEquals(6, header2.getLastLogId());
            assertEquals(entries2.getLast().term, header2.getLastLogTerm());
        }

        private List<RaftLogEntry> createRaftLogEntries(int count, int startId, int startTerm) {
            List<RaftLogEntry> entries = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                entries.add(new RaftLogEntry(startId + i, startTerm + i,
                        new PutCommand("key" + i, ("value" + i).getBytes())));
            }
            return entries;
        }
    }
}