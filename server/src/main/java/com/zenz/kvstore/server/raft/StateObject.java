package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class StateObject {

    volatile private State state;
    @Getter
    volatile private long currentTerm;
    volatile public long votedTerm;
    volatile private ReplicateTask replicateTask;
    volatile public Election election;

    private final Object currentTermLock = new Object();

    public StateObject() {
    }

    public void setCurrentTerm(final long currentTerm) {
        synchronized (this.currentTermLock) {
            this.currentTerm = Math.max(this.currentTerm, currentTerm);
        }
    }

    public static class ReplicateTask {

        public final RaftLogEntry logEntry;
        public final Future<Void> fut = new CompletableFuture<>();

        public ReplicateTask(final RaftLogEntry logEntry) {
            this.logEntry = logEntry;
        }
    }

    public static class Election {

        public final long term;
        public final long majority;
        public final long deadline;
        public long voteCount;

        public Election(final long term, final long majority, final long deadline) {
            this.term = term;
            this.majority = majority;
            this.deadline = deadline;
        }

        public boolean isExpired() {
            return System.currentTimeMillis() > this.deadline;
        }

        public boolean isDone() {
            return voteCount >= majority;
        }
    }
}
