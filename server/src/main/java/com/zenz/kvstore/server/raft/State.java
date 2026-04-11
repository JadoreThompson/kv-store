package com.zenz.kvstore.server.raft;

public enum State {

    FOLLOWER,
    CANDIDATE,
    LEADER
}
