package com.zenz.kvstore.server.logging;

import com.zenz.kvstore.common.command.DeleteCommand;
import com.zenz.kvstore.common.command.GetCommand;
import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.snapshot.KVStoreSnapshotter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotBody;
import com.zenz.kvstore.server.snapshot.RaftSnapshotFooter;
import com.zenz.kvstore.server.snapshot.RaftSnapshotHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RaftLogHandlerTest {

    private CommandLogger mockLogger;
    private KVStoreSnapshotter<RaftSnapshotHeader, RaftSnapshotBody, RaftSnapshotFooter> mockSnapshotter;
    private RaftLogHandler raftLogHandler;

    @BeforeEach
    void setUp() {
        mockLogger = mock(CommandLogger.class);
        mockSnapshotter = mock(KVStoreSnapshotter.class);
        raftLogHandler = new RaftLogHandler(mockLogger, mockSnapshotter);
    }

    @Test
    void log_firstCall_incrementsLogIdFromZero() throws Exception {
        PutCommand command = new PutCommand("key1", "value1".getBytes());
        raftLogHandler.setTerm(1);

        RaftLogEntry result = raftLogHandler.log(command);

        assertEquals(1, result.getId(), "First log should have ID 1");
        assertEquals(1, result.getTerm(), "First log should have term 1 (current term)");
        assertEquals(1, raftLogHandler.getLogId(), "Internal logId should be 1");
    }

    @Test
    void log_multipleCalls_incrementsLogIdSequentially() throws Exception {
        raftLogHandler.setTerm(1);
        
        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        RaftLogEntry secondEntry = raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));
        RaftLogEntry thirdEntry = raftLogHandler.log(new PutCommand("key3", "value3".getBytes()));

        assertEquals(2, secondEntry.getId(), "Second log should have ID 2");
        assertEquals(1, secondEntry.getTerm(), "Second log should have term 1 (the current term)");
        assertEquals(3, thirdEntry.getId(), "Third log should have ID 3");
        assertEquals(1, thirdEntry.getTerm(), "Third log should have term 1 (the current term)");
        assertEquals(3, raftLogHandler.getLogId(), "Internal logId should be 3");
    }

    @Test
    void log_includesTermInLogEntry() throws Exception {
        PutCommand command = new PutCommand("key1", "value1".getBytes());
        raftLogHandler.setTerm(5);

        RaftLogEntry result = raftLogHandler.log(command);

        assertEquals(5, result.getTerm(), "Log entry should have term 5");
    }

    @Test
    void log_termIsConsistentAcrossMultipleEntries() throws Exception {
        raftLogHandler.setTerm(10);

        RaftLogEntry entry1 = raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        RaftLogEntry entry2 = raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));
        RaftLogEntry entry3 = raftLogHandler.log(new PutCommand("key3", "value3".getBytes()));

        assertEquals(10, entry1.getTerm(), "First entry should have term 10");
        assertEquals(10, entry2.getTerm(), "Second entry should have term 10");
        assertEquals(10, entry3.getTerm(), "Third entry should have term 10");
    }

    @Test
    void log_differentTerms_eachEntryHasCorrectTerm() throws Exception {
        RaftLogEntry entry1 = raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.setTerm(2);
        RaftLogEntry entry2 = raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));
        raftLogHandler.setTerm(3);
        RaftLogEntry entry3 = raftLogHandler.log(new PutCommand("key3", "value3".getBytes()));

        assertEquals(0, entry1.getTerm(), "First entry should have default term 0");
        assertEquals(2, entry2.getTerm(), "Second entry should have term 2");
        assertEquals(3, entry3.getTerm(), "Third entry should have term 3");
    }

    @Test
    void log_command_callsLoggerWithLogEntry() throws Exception {
        PutCommand command = new PutCommand("key1", "value1".getBytes());
        raftLogHandler.setTerm(1);

        raftLogHandler.log(command);

        ArgumentCaptor<RaftLogEntry> captor = ArgumentCaptor.forClass(RaftLogEntry.class);
        verify(mockLogger).log(captor.capture());

        RaftLogEntry loggedEntry = captor.getValue();
        assertEquals(1, loggedEntry.getId(), "Log entry should have ID 1");
        assertEquals(1, loggedEntry.getTerm(), "Log entry should have term 1");
        assertEquals(command, loggedEntry.getCommand(), "Log entry should contain the command");
    }

    @Test
    void log_whenLogIdSet_startsFromThatId() throws Exception {
        raftLogHandler.setLogId(100);
        raftLogHandler.setTerm(5);

        RaftLogEntry result = raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));

        assertEquals(101, result.getId(), "Should start from 101 after starting at 100");
        assertEquals(5, result.getTerm(), "Should have term 5");
    }

    @Test
    void log_multipleCommands_allLoggedWithCorrectIdsAndTerms() throws Exception {
        raftLogHandler.setTerm(5);
        
        PutCommand cmd1 = new PutCommand("key1", "value1".getBytes());
        PutCommand cmd2 = new PutCommand("key2", "value2".getBytes());
        DeleteCommand cmd3 = new DeleteCommand("key3");

        RaftLogEntry entry1 = raftLogHandler.log(cmd1);
        RaftLogEntry entry2 = raftLogHandler.log(cmd2);
        RaftLogEntry entry3 = raftLogHandler.log(cmd3);

        assertEquals(1, entry1.getId());
        assertEquals(2, entry2.getId());
        assertEquals(3, entry3.getId());

        assertEquals(5, entry1.getTerm());
        assertEquals(5, entry2.getTerm());
        assertEquals(5, entry3.getTerm());

        ArgumentCaptor<RaftLogEntry> captor = ArgumentCaptor.forClass(RaftLogEntry.class);
        verify(mockLogger, times(3)).log(captor.capture());

        List<RaftLogEntry> allLogged = captor.getAllValues();
        assertEquals(1, allLogged.get(0).getId());
        assertEquals(2, allLogged.get(1).getId());
        assertEquals(3, allLogged.get(2).getId());
        assertEquals(5, allLogged.get(0).getTerm());
        assertEquals(5, allLogged.get(1).getTerm());
        assertEquals(5, allLogged.get(2).getTerm());
    }

    @Test
    void log_thresholdReached_callsSnapshotter() throws Exception {
        raftLogHandler.setLogsPerSnapshot(3);

        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));
        raftLogHandler.log(new PutCommand("key3", "value3".getBytes()));
        raftLogHandler.log(new PutCommand("key4", "value4".getBytes()));

        verify(mockSnapshotter, times(1)).snapshot(anyList());
    }

    @Test
    void log_thresholdNotReached_doesNotCallSnapshotter() throws Exception {
        raftLogHandler.setLogsPerSnapshot(5);

        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));

        verify(mockSnapshotter, never()).snapshot(anyList());
    }

    @Test
    void log_afterSnapshot_clearsEntriesAndContinues() throws Exception {
        raftLogHandler.setLogsPerSnapshot(2);

        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));
        raftLogHandler.log(new PutCommand("key3", "value3".getBytes()));

        verify(mockSnapshotter).snapshot(anyList());

        RaftLogEntry thirdEntry = raftLogHandler.log(new PutCommand("key4", "value4".getBytes()));

        assertEquals(4, thirdEntry.getId(), "Log ID should continue after snapshot");
    }

    @Test
    void log_snapshotterCalledWithCorrectEntries() throws Exception {
        raftLogHandler.setLogsPerSnapshot(2);
        raftLogHandler.setTerm(7);

        RaftLogEntry entry1 = raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        RaftLogEntry entry2 = raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));
        RaftLogEntry entry3 = raftLogHandler.log(new PutCommand("key3", "value3".getBytes()));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<RaftLogEntry>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockSnapshotter).snapshot(captor.capture());

        List<RaftLogEntry> snapshotEntries = captor.getValue();
        assertEquals(2, snapshotEntries.size(), "Snapshot should contain 2 entries");
        assertEquals(1, snapshotEntries.get(0).getId(), "First entry in snapshot should be ID 1");
        assertEquals(2, snapshotEntries.get(1).getId(), "Second entry in snapshot should be ID 2");
        assertEquals(7, snapshotEntries.get(0).getTerm(), "First entry in snapshot should have term 7");
        assertEquals(7, snapshotEntries.get(1).getTerm(), "Second entry in snapshot should have term 7");
    }

    @Test
    void log_withDeleteCommand_logsCorrectCommandType() throws Exception {
        DeleteCommand command = new DeleteCommand("keyToDelete");
        raftLogHandler.setTerm(1);

        RaftLogEntry result = raftLogHandler.log(command);

        ArgumentCaptor<RaftLogEntry> captor = ArgumentCaptor.forClass(RaftLogEntry.class);
        verify(mockLogger).log(captor.capture());

        assertNotNull(result.getCommand());
        assertEquals(command, result.getCommand());
    }

    @Test
    void getLogId_initiallyReturnsZero() {
        assertEquals(0, raftLogHandler.getLogId(), "Initial logId should be 0");
    }

    @Test
    void getTerm_initiallyReturnsZero() {
        assertEquals(0, raftLogHandler.getTerm(), "Initial term should be 0");
    }

    @Test
    void getLogsPerSnapshot_defaultValueIsHundredThousand() {
        assertEquals(100_000, raftLogHandler.getLogsPerSnapshot(), "Default logsPerSnapshot should be 100,000");
    }

    @Test
    void setLogsPerSnapshot_updatesValue() {
        raftLogHandler.setLogsPerSnapshot(50);
        assertEquals(50, raftLogHandler.getLogsPerSnapshot());
    }

    @Test
    void setTerm_updatesValue() {
        raftLogHandler.setTerm(42);
        assertEquals(42, raftLogHandler.getTerm());
    }

    @Test
    void log_snapshotTriggeredBeforeAddingEntry() throws Exception {
        raftLogHandler.setLogsPerSnapshot(2);

        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));
        raftLogHandler.log(new PutCommand("key3", "value3".getBytes()));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<RaftLogEntry>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockSnapshotter).snapshot(captor.capture());

        List<RaftLogEntry> snapshotEntries = captor.getValue();
        assertEquals(2, snapshotEntries.size());
    }

    @Test
    void getFirstEntry_whenEmpty_returnsNull() {
        assertNull(raftLogHandler.getFirstEntry(), "Should return null when no entries");
    }

    @Test
    void getFirstEntry_returnsFirstEntry() throws Exception {
        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));

        assertNotNull(raftLogHandler.getFirstEntry());
        assertEquals(1, raftLogHandler.getFirstEntry().getId());
    }

    @Test
    void getLastEntry_whenEmpty_returnsNull() {
        assertNull(raftLogHandler.getLastEntry(), "Should return null when no entries");
    }

    @Test
    void getLastEntry_returnsLastEntry() throws Exception {
        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));

        assertNotNull(raftLogHandler.getLastEntry());
        assertEquals(2, raftLogHandler.getLastEntry().getId());
    }

    @Test
    void log_afterSnapshot_updatesSeedEntry() throws Exception {
        raftLogHandler.setLogsPerSnapshot(2);
        raftLogHandler.setTerm(3);

        raftLogHandler.log(new PutCommand("key1", "value1".getBytes()));
        raftLogHandler.log(new PutCommand("key2", "value2".getBytes()));

        assertNotNull(raftLogHandler.getFirstEntry(), "First entry should be set after snapshot");
    }
}