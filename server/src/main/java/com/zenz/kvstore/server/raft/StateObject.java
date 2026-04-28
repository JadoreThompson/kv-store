package com.zenz.kvstore.server.raft;

import com.zenz.kvstore.server.logging.RaftLogEntry;
import com.zenz.kvstore.server.logging.RaftLogHandler;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.CompletableFuture;

public class StateObject {

    @Getter
    @Setter
    public volatile State state = State.FOLLOWER;

    @Getter
    private volatile long currentTerm;

    @Getter
    @Setter
    public volatile long votedTerm;

    @Getter
    @Setter
    public volatile ReplicateTask replicateTask;

    @Getter
    @Setter
    public volatile Election election;

    @Getter
    @Setter
    public volatile String leaderId;

    @Getter
    @Setter
    private volatile RaftLogHandler logHandler;

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

        @Getter
        private final RaftLogEntry logEntry;
        @Getter
        private final long prevLogId;
        @Getter
        private final long prevLogTerm;
        @Getter
        private final CompletableFuture<Boolean> future;
        @Getter
        private final int majority;

        private int count = 0;
        private final Object lock = new Object();

        public ReplicateTask(
                RaftLogEntry logEntry,
                long prevLogId,
                long prevLogTerm,
                CompletableFuture<Boolean> future,
                int majority
        ) {
            this.logEntry = logEntry;
            this.prevLogId = prevLogId;
            this.prevLogTerm = prevLogTerm;
            this.future = future;
            this.majority = majority;
        }

        public void incrementCount() {
            synchronized (lock) {
                ++count;
                if (count >= majority && !future.isDone()) {
                    future.complete(true);
                }
            }
        }

        public boolean isDone() {
            return future.isDone();
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
