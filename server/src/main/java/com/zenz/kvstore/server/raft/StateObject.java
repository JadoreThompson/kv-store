package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class StateObject {

    public volatile State state = State.FOLLOWER;

    @Getter
    private volatile long currentTerm;
    public volatile long votedTerm;
    public volatile ReplicateTask replicateTask;
    public volatile Election election;
    public volatile String leaderId;

    private final Object currentTermLock = new Object();

    public StateObject() {
    }

    public long setCurrentTerm(final long currentTerm) {
        synchronized (this.currentTermLock) {
            this.currentTerm = Math.max(this.currentTerm, currentTerm);
            return this.currentTerm;
        }
    }

    public static class ReplicateTask {

        public final RaftLogEntry logEntry;
        public final long prevLogTerm;
        public final long prevLogId;
        public final Future<Void> fut = new CompletableFuture<>();

        public ReplicateTask(final RaftLogEntry logEntry, final long prevLogId, final long prevLogTerm) {
            this.logEntry = logEntry;
            this.prevLogId = prevLogId;
            this.prevLogTerm = prevLogTerm;
        }
    }

    public static class Election {

        public final long term;
        public final long majority;
        public final long deadline;
        public volatile long voteCount;

        public Election(final long term, final long majority, final long deadline) {
            this.term = term;
            this.majority = majority;
            this.deadline = deadline;
        }

        public boolean isExpired() {
            return System.currentTimeMillis() >= this.deadline;
        }

        public boolean isDone() {
            return voteCount >= majority;
        }

        @Override
        public String toString() {
            return "Election{" +
                    "term=" + term +
                    ", majority=" + majority +
                    ", deadline=" + deadline +
                    ", voteCount=" + voteCount +
                    '}';
        }
    }
}
