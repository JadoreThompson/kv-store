package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotBody;
import com.zenz.kvstore.server.snapshot.SingleSnapshotFooter;
import com.zenz.kvstore.server.snapshot.SingleSnapshotHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class LogHandlerTest {

    private CommandLogger mockLogger;
    private KVStoreSnapshotter<SingleSnapshotHeader, SingleSnapshotBody, SingleSnapshotFooter> mockSnapshotter;
    private LogHandler logHandler;

    @BeforeEach
    void setUp() {
        mockLogger = mock(CommandLogger.class);
        mockSnapshotter = mock(KVStoreSnapshotter.class);
        logHandler = new LogHandler(mockLogger, mockSnapshotter);
    }

    @Test
    void log_firstCall_incrementsLogIdFromZero() throws Exception {
        PutCommand command = new PutCommand("key1", "value1".getBytes());

        LogEntry result = logHandler.log(command);

        assertEquals(1, result.getId(), "First log should have ID 1");
        assertEquals(1, logHandler.getLogId(), "Internal logId should be 1");
    }

    @Test
    void log_multipleCalls_incrementsLogIdSequentially() throws Exception {
        logHandler.log(new PutCommand("key1", "value1".getBytes()));
        LogEntry secondEntry = logHandler.log(new PutCommand("key2", "value2".getBytes()));
        LogEntry thirdEntry = logHandler.log(new PutCommand("key3", "value3".getBytes()));

        assertEquals(2, secondEntry.getId(), "Second log should have ID 2");
        assertEquals(3, thirdEntry.getId(), "Third log should have ID 3");
        assertEquals(3, logHandler.getLogId(), "Internal logId should be 3");
    }

    @Test
    void log_command_callsLoggerWithLogEntry() throws Exception {
        PutCommand command = new PutCommand("key1", "value1".getBytes());

        logHandler.log(command);

        ArgumentCaptor<LogEntry> captor = ArgumentCaptor.forClass(LogEntry.class);
        verify(mockLogger).log(captor.capture());

        LogEntry loggedEntry = captor.getValue();
        assertEquals(1, loggedEntry.getId(), "Log entry should have ID 1");
        assertEquals(command, loggedEntry.getCommand(), "Log entry should contain the command");
    }

    @Test
    void log_whenLogIdSet_startsFromThatId() throws Exception {
        logHandler.setLogId(100);

        LogEntry result = logHandler.log(new PutCommand("key1", "value1".getBytes()));

        assertEquals(101, result.getId(), "Should start from 101 after starting at 100");
    }

    @Test
    void log_multipleCommands_allLoggedWithCorrectIds() throws Exception {
        PutCommand cmd1 = new PutCommand("key1", "value1".getBytes());
        PutCommand cmd2 = new PutCommand("key2", "value2".getBytes());
        DeleteCommand cmd3 = new DeleteCommand("key3");

        LogEntry entry1 = logHandler.log(cmd1);
        LogEntry entry2 = logHandler.log(cmd2);
        LogEntry entry3 = logHandler.log(cmd3);

        assertEquals(1, entry1.getId());
        assertEquals(2, entry2.getId());
        assertEquals(3, entry3.getId());

        ArgumentCaptor<LogEntry> captor = ArgumentCaptor.forClass(LogEntry.class);
        verify(mockLogger, times(3)).log(captor.capture());

        List<LogEntry> allLogged = captor.getAllValues();
        assertEquals(1, allLogged.get(0).getId());
        assertEquals(2, allLogged.get(1).getId());
        assertEquals(3, allLogged.get(2).getId());
    }

    @Test
    void log_thresholdReached_callsSnapshotter() throws Exception {
        logHandler.setLogsPerSnapshot(3);

        logHandler.log(new PutCommand("key1", "value1".getBytes()));
        logHandler.log(new PutCommand("key2", "value2".getBytes()));
        logHandler.log(new PutCommand("key3", "value3".getBytes()));

        verify(mockSnapshotter, times(1)).snapshot(anyList());
    }

    @Test
    void log_thresholdNotReached_doesNotCallSnapshotter() throws Exception {
        logHandler.setLogsPerSnapshot(5);

        logHandler.log(new PutCommand("key1", "value1".getBytes()));
        logHandler.log(new PutCommand("key2", "value2".getBytes()));

        verify(mockSnapshotter, never()).snapshot(anyList());
    }

    @Test
    void log_afterSnapshot_resetsEntriesAndContinues() throws Exception {
        logHandler.setLogsPerSnapshot(2);

        logHandler.log(new PutCommand("key1", "value1".getBytes()));
        logHandler.log(new PutCommand("key2", "value2".getBytes()));

        verify(mockSnapshotter).snapshot(anyList());
        assertEquals(0, logHandler.getEntries().size(), "Entry buffer should be cleared after taking a snapshot");
        LogEntry thirdEntry = logHandler.log(new PutCommand("key3", "value3".getBytes()));

        assertEquals(3, thirdEntry.getId(), "Log ID should continue after snapshot");
    }

    @Test
    void log_snapshotterCalledWithCorrectEntries() throws Exception {
        logHandler.setLogsPerSnapshot(2);

        LogEntry entry1 = logHandler.log(new PutCommand("key1", "value1".getBytes()));
        LogEntry entry2 = logHandler.log(new PutCommand("key2", "value2".getBytes()));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<LogEntry>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockSnapshotter).snapshot(captor.capture());

        List<LogEntry> snapshotEntries = captor.getValue();
        assertEquals(2, snapshotEntries.size(), "Snapshot should contain 2 entries");
        assertEquals(1, snapshotEntries.get(0).getId(), "First entry in snapshot should be ID 1");
        assertEquals(2, snapshotEntries.get(1).getId(), "Second entry in snapshot should be ID 2");
    }

    @Test
    void log_withDeleteCommand_logsCorrectCommandType() throws Exception {
        DeleteCommand command = new DeleteCommand("keyToDelete");

        LogEntry result = logHandler.log(command);

        ArgumentCaptor<LogEntry> captor = ArgumentCaptor.forClass(LogEntry.class);
        verify(mockLogger).log(captor.capture());

        assertNotNull(result.getCommand());
        assertEquals(command, result.getCommand());
    }

    @Test
    void getLogId_initiallyReturnsZero() {
        assertEquals(0, logHandler.getLogId(), "Initial logId should be 0");
    }

    @Test
    void getLogsPerSnapshot_defaultValueIsHundredThousand() {
        assertEquals(100_000, logHandler.getLogsPerSnapshot(), "Default logsPerSnapshot should be 100,000");
    }

    @Test
    void setLogsPerSnapshot_updatesValue() {
        logHandler.setLogsPerSnapshot(50);
        assertEquals(50, logHandler.getLogsPerSnapshot());
    }
}