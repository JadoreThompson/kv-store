package com.zenz.kvstore.server.restore;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.*;
import com.zenz.kvstore.server.snapshot.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KVStoreRestorerTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("KVStoreRestorer Tests")
    class SingleKVStoreRestorerTests {

        private Path snapshotDir;
        private Path logDir;

        @BeforeEach
        void setUp() throws Exception {
            snapshotDir = Files.createTempDirectory("test-snapshots-");
            logDir = Files.createTempDirectory("test-logs-");
        }

        @AfterEach
        void tearDown() throws Exception {
            if (Files.exists(snapshotDir)) {
                snapshotDir.toFile().delete();
            }
            if (Files.exists(logDir)) {
                logDir.toFile().delete();
            }
        }

        @Test
        void restore_withLogsAfterSnapshot_restoresAllKeysAndLogs() throws Exception {
            Path snapshotPath = snapshotDir.resolve("3.snapshot");

            KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter =
                    createSingleSnapshotter();
            snapshotter.setDir(snapshotDir);

            List<LogEntry> snapshotEntries = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                snapshotEntries.add(new LogEntry(i + 1, new PutCommand("key" + i, ("value" + i).getBytes())));
            }
            snapshotter.snapshot(snapshotEntries);

            Path logFile = logDir.resolve("app.log");
            WALogger walogger = new WALogger(logFile);
            LogHandler logHandler = new LogHandler(walogger, snapshotter);

            logHandler.log(new PutCommand("key3", "value3".getBytes()));
            logHandler.log(new PutCommand("key4", "value4".getBytes()));
            logHandler.log(new PutCommand("key5", "value5".getBytes()));
            walogger.close();

            KVStoreRestorer restorer = new KVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new LogHandler(
                    new WALogger(logFile), snapshotter)));

            assertNotNull(restoredStore.getTrie().get("key0"));
            assertNotNull(restoredStore.getTrie().get("key1"));
            assertNotNull(restoredStore.getTrie().get("key2"));
            assertNotNull(restoredStore.getTrie().get("key3"));
            assertNotNull(restoredStore.getTrie().get("key4"));
            assertNotNull(restoredStore.getTrie().get("key5"));

            LogHandler restoredLogHandler = (LogHandler) restoredStore.getLogHandler();
            assertNotNull(restoredLogHandler);
        }

        @Test
        void restore_withPartialLogs_logsReplenishedCorrectly() throws Exception {
            Path snapshotPath = snapshotDir.resolve("5.snapshot");

            KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter =
                    createSingleSnapshotter();
            snapshotter.setDir(snapshotDir);

            List<LogEntry> snapshotEntries = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                snapshotEntries.add(new LogEntry(i, new PutCommand("key" + i, ("value" + i).getBytes())));
            }
            snapshotter.snapshot(snapshotEntries);

            Path logFile = logDir.resolve("app.log");
            WALogger walogger = new WALogger(logFile);
            LogHandler logHandler = new LogHandler(walogger, snapshotter);
            logHandler.setLogId(5);

            logHandler.log(new PutCommand("key6", "value6".getBytes()));
            logHandler.log(new PutCommand("key7", "value7".getBytes()));
            walogger.close();

            KVStoreRestorer restorer = new KVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new LogHandler(
                    new WALogger(logFile), snapshotter)));

            assertNotNull(restoredStore.getTrie().get("key6"));
            assertNotNull(restoredStore.getTrie().get("key7"));

            assertEquals(7, restoredStore.getLogHandler().getLogId());
        }

        @Test
        void restore_withDeletedKeys_deletesAreRestored() throws Exception {
            Path snapshotPath = snapshotDir.resolve("5.snapshot");

            KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter =
                    createSingleSnapshotter();
            snapshotter.setDir(snapshotDir);

            List<LogEntry> entries = new ArrayList<>();
            entries.add(new LogEntry(1, new PutCommand("key1", "value1".getBytes())));
            entries.add(new LogEntry(2, new PutCommand("key2", "value2".getBytes())));
            entries.add(new LogEntry(3, new PutCommand("key3", "value3".getBytes())));
            entries.add(new LogEntry(4, new DeleteCommand("key2")));
            entries.add(new LogEntry(5, new PutCommand("key4", "value4".getBytes())));
            snapshotter.snapshot(entries);

            Path logFile = logDir.resolve("app.log");
            WALogger walogger = new WALogger(logFile);
            LogHandler logHandler = new LogHandler(walogger, snapshotter);

            logHandler.log(new PutCommand("key5", "value5".getBytes()));
            walogger.close();

            KVStoreRestorer restorer = new KVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new LogHandler(new WALogger(logFile), snapshotter)));

            assertNotNull(restoredStore.getTrie().get("key1"));
            assertNull(restoredStore.getTrie().get("key2"), "key2 should be deleted");
            assertNotNull(restoredStore.getTrie().get("key3"));
            assertNotNull(restoredStore.getTrie().get("key4"));
            assertNotNull(restoredStore.getTrie().get("key5"));
        }

        @Test
        void restore_withOnlySnapshots_noLogsNeeded() throws Exception {
            Path snapshotPath = snapshotDir.resolve("3.snapshot");

            KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter =
                    createSingleSnapshotter();
            snapshotter.setDir(snapshotDir);

            List<LogEntry> entries = new ArrayList<>();
            entries.add(new LogEntry(1, new PutCommand("key1", "value1".getBytes())));
            entries.add(new LogEntry(2, new PutCommand("key2", "value2".getBytes())));
            entries.add(new LogEntry(3, new PutCommand("key3", "value3".getBytes())));
            snapshotter.snapshot(entries);

            Path logFile = logDir.resolve("empty.log");
            Files.createFile(logFile);

            KVStoreRestorer restorer = new KVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new LogHandler(new WALogger(logFile), snapshotter)));

            assertNotNull(restoredStore.getTrie().get("key1"));
            assertNotNull(restoredStore.getTrie().get("key2"));
            assertNotNull(restoredStore.getTrie().get("key3"));
        }

        @Test
        void restore_afterSnapshot_logEntriesShouldBeEmpty() throws Exception {
            KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter =
                    createSingleSnapshotter();
            snapshotter.setDir(snapshotDir);

            // Create snapshot with entries
            List<LogEntry> snapshotEntries = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                snapshotEntries.add(new LogEntry(i,
                        new PutCommand("key" + i, ("value" + i).getBytes())));
            }
            snapshotter.snapshot(snapshotEntries);

            KVStoreRestorer restorer = new KVStoreRestorer();
            KVStore restoredStore = restorer.restore(
                    new KVStore(new LogHandler(new WALogger(), snapshotter))
            );

            LogHandler restoredLogHandler = (LogHandler) restoredStore.getLogHandler();

            assertTrue(restoredLogHandler.getEntries().isEmpty(),
                    "Log entries should be empty after restoring from snapshot");
        }
    }

    @Nested
    @DisplayName("RaftKVStoreRestorer Tests")
    class RaftKVStoreRestorerTests {

        private Path snapshotDir;
        private Path logDir;

        @BeforeEach
        void setUp() throws Exception {
            snapshotDir = Files.createTempDirectory("raft-test-snapshots-");
            logDir = Files.createTempDirectory("raft-test-logs-");
        }

        @AfterEach
        void tearDown() throws Exception {
            if (Files.exists(snapshotDir)) {
                snapshotDir.toFile().delete();
            }
            if (Files.exists(logDir)) {
                logDir.toFile().delete();
            }
        }

        @Test
        void restore_withTerm_restoresCorrectTerm() throws Exception {
            Path snapshotPath = snapshotDir.resolve("3.snapshot");

            KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter =
                    createRaftSnapshotter();
            snapshotter.setDir(snapshotDir);

            List<RaftLogEntry> entries = new ArrayList<>();
            entries.add(new RaftLogEntry(1, 5, new PutCommand("key1", "value1".getBytes())));
            entries.add(new RaftLogEntry(2, 5, new PutCommand("key2", "value2".getBytes())));
            entries.add(new RaftLogEntry(3, 5, new PutCommand("key3", "value3".getBytes())));
            assertEquals(snapshotPath, snapshotter.snapshot(entries));

            Path logFile = logDir.resolve("app.log");
            WALogger walogger = new WALogger(logFile);
            RaftLogHandler logHandler = new RaftLogHandler(walogger, snapshotter);

            logHandler.setTerm(7);
            logHandler.log(new PutCommand("key4", "value4".getBytes()));
            logHandler.log(new PutCommand("key5", "value5".getBytes()));
            walogger.close();

            RaftKVStoreRestorer restorer = new RaftKVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new RaftLogHandler(new WALogger(logFile), snapshotter)));

            RaftLogHandler restoredLogHandler = (RaftLogHandler) restoredStore.getLogHandler();
            assertEquals(7, restoredLogHandler.getTerm(), "Term should be restored from last log entry");
        }

        @Test
        void restore_withMultipleTerms_restoresHighestTerm() throws Exception {
            Path snapshotPath = snapshotDir.resolve("3.snapshot");

            KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter =
                    new KVStoreSnapshotter<>(RaftSnapshotHeader.class, RaftSnapshotBody.class, RaftSnapshotFooter.class);
            snapshotter.setDir(snapshotDir);

            List<RaftLogEntry> entries = new ArrayList<>();
            entries.add(new RaftLogEntry(1, 3, new PutCommand("key1", "value1".getBytes())));
            entries.add(new RaftLogEntry(2, 5, new PutCommand("key2", "value2".getBytes())));
            entries.add(new RaftLogEntry(3, 5, new PutCommand("key3", "value3".getBytes())));
            assertEquals(snapshotPath, snapshotter.snapshot(entries));

            Path logFile = logDir.resolve("app.log");
            WALogger walogger = new WALogger(logFile);

            assertTrue(Files.exists(logFile), "Log file should have been created");

            RaftLogHandler logHandler = new RaftLogHandler(walogger, snapshotter);
            logHandler.setTerm(10);
            logHandler.log(new PutCommand("key4", "value4".getBytes()));
            walogger.close();

            RaftKVStoreRestorer restorer = new RaftKVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new RaftLogHandler(new WALogger(logFile), snapshotter)));

            RaftLogHandler restoredLogHandler = (RaftLogHandler) restoredStore.getLogHandler();
            assertEquals(10, restoredLogHandler.getTerm());

            assertNotNull(restoredStore.getTrie().get("key1"));
            assertNotNull(restoredStore.getTrie().get("key2"));
            assertNotNull(restoredStore.getTrie().get("key3"));
            assertNotNull(restoredStore.getTrie().get("key4"));
        }

        @Test
        void restore_withLogsAndSnapshots_restoresAllKeysAndLogState() throws Exception {
            Path snapshotPath = snapshotDir.resolve("2.snapshot");

            KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter = createRaftSnapshotter();
            snapshotter.setDir(snapshotDir);

            List<RaftLogEntry> entries = new ArrayList<>();
            entries.add(new RaftLogEntry(1, 1, new PutCommand("key1", "value1".getBytes())));
            entries.add(new RaftLogEntry(2, 1, new PutCommand("key2", "value2".getBytes())));
            snapshotter.snapshot(entries);

            Path logFile = logDir.resolve("app.log");
            WALogger walogger = new WALogger(logFile);
            RaftLogHandler logHandler = new RaftLogHandler(walogger, snapshotter);

            logHandler.setTerm(3);
            logHandler.setLogId(entries.getLast().id);
            logHandler.log(new PutCommand("key3", "value3".getBytes()));
            logHandler.log(new PutCommand("key4", "value4".getBytes()));
            logHandler.log(new PutCommand("key5", "value5".getBytes()));
            walogger.close();

            RaftKVStoreRestorer restorer = new RaftKVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new RaftLogHandler(new WALogger(logFile), snapshotter)));

            assertNotNull(restoredStore.getTrie().get("key1"));
            assertNotNull(restoredStore.getTrie().get("key2"));
            assertNotNull(restoredStore.getTrie().get("key3"));
            assertNotNull(restoredStore.getTrie().get("key4"));
            assertNotNull(restoredStore.getTrie().get("key5"));

            RaftLogHandler restoredLogHandler = (RaftLogHandler) restoredStore.getLogHandler();
            assertEquals(3, restoredLogHandler.getTerm());
            assertEquals(5, restoredLogHandler.getLogId());
        }

        @Test
        void restore_emptyLog_stillRestoresFromSnapshot() throws Exception {
            Path snapshotPath = snapshotDir.resolve("2.snapshot");

            KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter =
                    createRaftSnapshotter();
            snapshotter.setDir(snapshotDir);

            List<RaftLogEntry> entries = new ArrayList<>();
            entries.add(new RaftLogEntry(1, 2, new PutCommand("key1", "value1".getBytes())));
            entries.add(new RaftLogEntry(2, 2, new PutCommand("key2", "value2".getBytes())));
            snapshotter.snapshot(entries);

            Path logFile = logDir.resolve("empty.log");
            Files.createFile(logFile);

            RaftKVStoreRestorer restorer = new RaftKVStoreRestorer();
            KVStore restoredStore = restorer.restore(new KVStore(new RaftLogHandler(new WALogger(logFile), snapshotter)));

            assertNotNull(restoredStore.getTrie().get("key1"));
            assertNotNull(restoredStore.getTrie().get("key2"));

            RaftLogHandler restoredLogHandler = (RaftLogHandler) restoredStore.getLogHandler();
            assertEquals(0, restoredLogHandler.getTerm(), "Term should be 0 for empty log");
        }

        @Test
        void restore_afterSnapshot_logEntriesShouldBeEmpty() throws Exception {
            KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> snapshotter =
                    createRaftSnapshotter();
            snapshotter.setDir(snapshotDir);

            // Create snapshot with entries
            List<RaftLogEntry> entries = new ArrayList<>();
            entries.add(new RaftLogEntry(1, 1, new PutCommand("key1", "value1".getBytes())));
            entries.add(new RaftLogEntry(2, 1, new PutCommand("key2", "value2".getBytes())));
            snapshotter.snapshot(entries);

            // No logs after snapshot
            Path logFile = logDir.resolve("empty.log");
            Files.createFile(logFile);

            RaftKVStoreRestorer restorer = new RaftKVStoreRestorer();
            KVStore restoredStore = restorer.restore(
                    new KVStore(new RaftLogHandler(new WALogger(logFile), snapshotter))
            );

            RaftLogHandler restoredLogHandler =
                    (RaftLogHandler) restoredStore.getLogHandler();

            assertTrue(restoredLogHandler.getEntries().isEmpty(),
                    "Raft log entries should be empty after restoring from snapshot");
        }
    }

    private KVStoreSnapshotter<
            SingleSnapshotHeader,
            SingleSnapshotBody,
            SingleSnapshotFooter> createSingleSnapshotter() {
        return new KVStoreSnapshotter<>(
                SingleSnapshotHeader.class, SingleSnapshotBody.class, SingleSnapshotFooter.class);
    }

    private KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> createRaftSnapshotter() {
        return new KVStoreSnapshotter<>(
                RaftSnapshotHeader.class, RaftSnapshotBody.class, RaftSnapshotFooter.class);
    }
}