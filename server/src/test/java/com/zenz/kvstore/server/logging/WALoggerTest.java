package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotBody;
import com.zenz.kvstore.server.snapshot.SingleSnapshotFooter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WALoggerTest {

    private Path tempLogFile;
    private WALogger walogger;
    private LogHandler logHandler;
    private KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> snapshotter;

    @BeforeEach
    void setUp() throws Exception {
        tempLogFile = Files.createTempFile("walogger-test-", ".log");
        walogger = new WALogger(tempLogFile);

        snapshotter = new KVStoreSnapshotter<>(
                SingleSnapshotHeader.class,
                SingleSnapshotBody.class,
                SingleSnapshotFooter.class
        );
        snapshotter.setDir(Files.createTempDirectory("temp-snapshots-"));

        logHandler = new LogHandler(walogger, snapshotter);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (walogger != null) {
            walogger.close();
        }
        if (tempLogFile != null && Files.exists(tempLogFile)) {
            Files.delete(tempLogFile);
        }
    }

    @Test
    void logPutCommand_canBeLoadedBackSuccessfully() throws Exception {
        PutCommand putCommand = new PutCommand("key1", "value1".getBytes());
        LogEntry loggedEntry = logHandler.log(putCommand);

        List<LogEntry> loadedEntries = logHandler.loadLogs(tempLogFile);

        assertEquals(1, loadedEntries.size(), "Should have loaded exactly one entry");

        LogEntry loadedEntry = loadedEntries.getFirst();
        assertEquals(loggedEntry.getId(), loadedEntry.getId(), "Log IDs should match");

        CommandType loadedCommandType = loadedEntry.getCommand().type();
        assertEquals(CommandType.PUT, loadedCommandType, "Command type should be PUT");

        PutCommand loadedPutCommand = (PutCommand) loadedEntry.getCommand();
        assertEquals("key1", loadedPutCommand.key(), "Key should match");
        assertArrayEquals("value1".getBytes(), loadedPutCommand.value(), "Value should match");
    }

    @Test
    void logMultipleCommands_canBeLoadedBackInCorrectOrder() throws Exception {
        int numEntries = 5;
        long[] loggedIds = new long[numEntries];

        for (int i = 0; i < numEntries; i++) {
            PutCommand putCommand = new PutCommand("key" + i, ("value" + i).getBytes());
            LogEntry entry = logHandler.log(putCommand);
            loggedIds[i] = entry.getId();
        }

        List<LogEntry> loadedEntries = logHandler.loadLogs(tempLogFile);

        assertEquals(numEntries, loadedEntries.size(), "Should have loaded all entries");

        for (int i = 0; i < numEntries; i++) {
            assertEquals(loggedIds[i], loadedEntries.get(i).getId(), "Log ID at index " + i + " should match");

            PutCommand loadedCommand = (PutCommand) loadedEntries.get(i).getCommand();
            assertEquals("key" + i, loadedCommand.key(), "Key at index " + i + " should match");
            assertArrayEquals(("value" + i).getBytes(), loadedCommand.value(), "Value at index " + i + " should match");
        }
    }

    @Test
    void logDeleteCommand_canBeLoadedBackSuccessfully() throws Exception {
        DeleteCommand deleteCommand = new DeleteCommand("keyToDelete");
        LogEntry loggedEntry = logHandler.log(deleteCommand);

        List<LogEntry> loadedEntries = logHandler.loadLogs(tempLogFile);

        assertEquals(1, loadedEntries.size(), "Should have loaded exactly one entry");

        LogEntry loadedEntry = loadedEntries.getFirst();
        assertEquals(loggedEntry.getId(), loadedEntry.getId(), "Log IDs should match");

        CommandType loadedCommandType = loadedEntry.getCommand().type();
        assertEquals(CommandType.DELETE, loadedCommandType, "Command type should be DELETE");

        DeleteCommand loadedDeleteCommand = (DeleteCommand) loadedEntry.getCommand();
        assertEquals("keyToDelete", loadedDeleteCommand.key(), "Key should match");
    }

    @Test
    void logMixedCommands_allCanBeLoadedBackWithCorrectTypes() throws Exception {
        logHandler.log(new PutCommand("key1", "value1".getBytes()));
        logHandler.log(new DeleteCommand("key1"));
        logHandler.log(new PutCommand("key2", "value2".getBytes()));

        List<LogEntry> loadedEntries = logHandler.loadLogs(tempLogFile);

        assertEquals(3, loadedEntries.size(), "Should have loaded all 3 entries");

        assertEquals(CommandType.PUT, loadedEntries.getFirst().getCommand().type(), "First entry should be PUT");
        assertEquals(CommandType.DELETE, loadedEntries.get(1).getCommand().type(), "Second entry should be DELETE");
        assertEquals(CommandType.PUT, loadedEntries.get(2).getCommand().type(), "Third entry should be PUT");
    }

    @Test
    void loadLogsFromEmptyFile_returnsEmptyList() throws Exception {
        Path emptyFile = Files.createTempFile("empty-log-", ".log");

        try {
            List<LogEntry> loadedEntries = logHandler.loadLogs(emptyFile);

            assertTrue(loadedEntries.isEmpty(), "Empty file should return empty list");
        } finally {
            Files.delete(emptyFile);
        }
    }

    @Test
    void logCommand_fileContainsData() throws Exception {
        logHandler.log(new PutCommand("testKey", "testValue".getBytes()));

        long fileSize = Files.size(tempLogFile);
        assertTrue(fileSize > 0, "Log file should contain data after logging");
    }
}