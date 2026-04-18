package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.common.command.PutCommand;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import com.zenz.kvstore.server.raft.message.AppendEntry;
import com.zenz.kvstore.server.raft.message.AppendEntryResponse;
import com.zenz.kvstore.server.raft.message.Message;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

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
    private RaftLogHandler mockLogHandler;

    @BeforeEach
    public void setup() {
        server = new Server(new InetSocketAddress("localhost", 9999));
    }

    @Test
    public void testSamePrevLogId_andPrevLogTerm() {
        final List<RaftLogEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries.add(new RaftLogEntry(
                    i + 1,
                    1,
                    new PutCommand("key-" + (i + 1), "value".getBytes(StandardCharsets.UTF_8))));
        }
        final AppendEntry appendEntry = new AppendEntry(
                "leader",
                1L,
                0L,
                0L,
                entries);

        when(mockManager.getNodeConfig()).thenReturn(new NodeConfig(
                "follower",
                new InetSocketAddress("localhost", 9999)));
        doReturn(mockLogHandler).when(mockKvstore).getLogHandler();
        doReturn(mockKvstore).when(mockManager).getKvstore();
        when(mockStateObject.getCurrentTerm()).thenReturn(1L);
        when(mockStateObject.getLeaderId()).thenReturn(null);
        when(mockLogHandler.getSeedEntry()).thenReturn(new RaftLogEntry(
                0L,
                0L,
                new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8))));
        server.setManager(mockManager);
        server.setStateObject(mockStateObject);

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
        doReturn(mockLogHandler).when(mockKvstore).getLogHandler();
        doReturn(mockKvstore).when(mockManager).getKvstore();
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
        doReturn(mockLogHandler).when(mockKvstore).getLogHandler();
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
        doReturn(mockLogHandler).when(mockKvstore).getLogHandler();
        doReturn(mockKvstore).when(mockManager).getKvstore();
        when(mockStateObject.getCurrentTerm()).thenReturn(1L);
        when(mockStateObject.getLeaderId()).thenReturn(null);
        when(mockLogHandler.getSeedEntry()).thenReturn(new RaftLogEntry(
                0L,
                0L,
                new PutCommand("seed", "value".getBytes(StandardCharsets.UTF_8))));
        when(mockLogHandler.getLogId()).thenReturn(0L);
        when(mockLogHandler.getTerm()).thenReturn(0L);
        server.setManager(mockManager);
        server.setStateObject(mockStateObject);

        final Message response = server.handleAppendEntry(appendEntry);
        assertInstanceOf(AppendEntryResponse.class, response);

        final AppendEntryResponse appendEntryResponse = (AppendEntryResponse) response;
        assertTrue(appendEntryResponse.isSuccess());
        assertNull(appendEntryResponse.failureReason());
    }
}
