package com.zenz.kvstore.raft;


public class KVRaft {
    private KVRaftBroker broker;
    private KVRaftController controller;

    public KVRaftBroker getBroker() {
        return broker;
    }

    public void setBroker(KVRaftBroker broker) {
        this.broker = broker;
    }

    public KVRaftController getController() {
        return controller;
    }

    public void setController(KVRaftController controller) {
        this.controller = controller;
    }

    public KVRaftController convert(KVRaftBroker broker) {
        if (!broker.equals(this.broker)) return null;
        controller = new KVRaftController(broker);
        broker = null;
        return controller;
    }
}
