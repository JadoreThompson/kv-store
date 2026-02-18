package com.zenz.kvstore;

import java.util.Objects;

public class KVMap {
    public static int INITIAL_CAPACITY = 1000;
    public static float LOAD_FACTOR = 1.0f;
    public static int REHASH_BUCKETS = 100;
    private int capacity;
    private Array<NodeList> ht1;
    private Array<NodeList> ht2;
    private int rehashIdx;

    public KVMap() {
        capacity = INITIAL_CAPACITY;
        ht1 = new Array<>(capacity);
        rehashIdx = -1;
    }

    public Node get(String key) {
        int hashCode = key.hashCode();
        int idx;

        if (checkIsRehashing()) {
            rehash();
            idx = bucketIndex(hashCode, ht2.length());
            NodeList nodeList = ht2.get(idx);
            if (nodeList != null) {
                return find(key, nodeList);
            }
        }

        idx = bucketIndex(hashCode, ht1.length());
        NodeList nodeList = ht1.get(idx);
        if (nodeList != null) {
            return find(key, nodeList);
        }

        return null;
    }

    public void put(String key, byte[] value) {
        int hashCode = key.hashCode();
        int idx;
        NodeList nodeList;

        if (checkIsRehashing()) {
            idx = bucketIndex(hashCode, ht2.length());
            nodeList = ht2.get(idx);
            if (nodeList == null) {
                ht2.add(idx, new NodeList(new Node(key, value)));
                return;
            }
        } else {
            idx = bucketIndex(hashCode, ht1.length());
            nodeList = ht1.get(idx);
            if (nodeList == null) {
                ht1.add(idx, new NodeList(new Node(key, value)));
                return;
            }
        }

        Node cur = nodeList.head;

        if (cur.key.equals(key)) {
            cur.value = value;
            return;
        }

        while (cur.next != null) {
            if (cur.key.equals(key)) {
                cur.value = value;
                return;
            }
            cur = cur.next;
        }

        cur.next = new Node(key, value);
    }

    private Node find(String key, NodeList nodeList) {
        Node cur = nodeList.head;

        while (cur != null) {
            if (cur.key.equals(key)) {
                return cur;
            }
            cur = cur.next;
        }

        return null;
    }

    public void rehash() {
        for (int i = 0; i < Math.min(ht1.length(), REHASH_BUCKETS); i++) {
            NodeList nodeList = ht1.get(rehashIdx);

            if (nodeList != null) {
                String key = nodeList.head.key;
                int newHashIdx = bucketIndex(key.hashCode(), ht2.length());
                ht2.add(newHashIdx, nodeList);
            }

            rehashIdx += 1;
        }

        if (rehashIdx >= ht1.length()) {
            rehashIdx = -1;
            ht1 = ht2;
            ht2 = null;
        }
    }


    /**
     * Returns a non-negative bucket index.
     * Note: Forcing the sign bit to 0.
     */
    private int bucketIndex(int hashCode, int len) {
        return (hashCode & 0x7fffffff) % len;
    }

    private boolean checkIsRehashing() {
        if (rehashIdx >= 0) {
            return true;
        }

        if (ht1.size() >= (int) (capacity * LOAD_FACTOR)) {
            capacity *= capacity;
            ht2 = new Array<>(capacity);
            rehashIdx = 0;
            return true;
        }

        return false;
    }

    public static class Node {
        public final String key;
        public byte[] value;
        public Node next;

        public Node(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof Node other)) return false;
            return Objects.equals(this.key, other.key);
        }
    }

    private static class NodeList {
        public Node head;

        public NodeList(Node node) {
            head = node;
        }
    }
}
