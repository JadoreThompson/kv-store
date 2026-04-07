package com.zenz.kvstore.server;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class KVMap {

    public static float LOAD_FACTOR = 1.0f;
    public static int REHASH_BUCKETS = 100;

    private final float loadFactor;
    private final int rehashBuckets;
    private KVArray ht1;
    private KVArray ht2;
    private int rehashIdx = -1;

    public KVMap() {
        this(LOAD_FACTOR, REHASH_BUCKETS);
    }

    public KVMap(float loadFactor, int rehashBuckets) {
        this.loadFactor = loadFactor;
        this.rehashBuckets = rehashBuckets;
        this.ht1 = new KVArray();
    }

    public void put(String key, byte[] value) {
        int hashCode = key.hashCode();

        if (isRehashing()) {
            rehash();

            // Inserting into ht1
            int idx = bucketIndex(hashCode, this.ht1.length());
            NodeList nl = this.ht1.get(idx);
            if (nl != null) {
                Node node = this.findNode(key, nl);
                if (node != null) {
                    node.value = value;
                } else {
                    nl.add(new Node(key, value));
                }
                return;
            }

            // Inserting into ht2
            idx = bucketIndex(hashCode, this.ht2.length());
            nl = this.ht2.get(idx);
            if (nl == null) {
                nl = new NodeList(this.ht2);
                this.ht2.set(idx, nl);
            }

            Node node = this.findNode(key, nl);
            if (node != null) {
                node.value = value;
            } else {
                nl.add(new Node(key, value));
            }
        } else {
            // Inserting into ht1
            int idx = bucketIndex(hashCode, this.ht1.length());
            NodeList nl = this.ht1.get(idx);
            if (nl == null) {
                nl = new NodeList(this.ht1);
                this.ht1.set(idx, nl);
            }

            Node node = this.findNode(key, nl);
            if (node != null) {
                node.value = value;
            } else {
                nl.add(new Node(key, value));
            }
        }
    }

    public Node get(String key) {
        int hashCode = key.hashCode();

        if (isRehashing()) {
            rehash();

            // Checking ht1
            int idx = bucketIndex(hashCode, this.ht1.length());
            NodeList nl = this.ht1.get(idx);
            if (nl != null) return this.findNode(key, nl);

            // Checking ht2
            idx = bucketIndex(hashCode, this.ht2.length());
            nl = this.ht2.get(idx);
            return nl == null ? null : this.findNode(key, nl);
        }

        int idx = bucketIndex(hashCode, this.ht1.length());
        NodeList nl = this.ht1.get(idx);
        return nl == null ? null : this.findNode(key, nl);
    }

    public boolean remove(String key) {
        int hashCode = key.hashCode();

        if (isRehashing()) {
            rehash();

            // Checking ht1
            int idx = bucketIndex(hashCode, this.ht1.length());
            NodeList nl = this.ht1.get(idx);
            if (nl != null) {
                Node node = this.findNode(key, nl);
                if (node == null) return false;
                nl.remove(node);
                return true;
            }

            // Checking ht2
            idx = bucketIndex(hashCode, this.ht2.length());
            nl = this.ht2.get(idx);
            if (nl == null) return false;

            Node node = this.findNode(key, nl);
            if (node == null) return false;
            nl.remove(node);
            return true;
        }

        // Checking ht1
        int idx = bucketIndex(hashCode, this.ht1.length());
        NodeList nl = this.ht1.get(idx);
        if (nl == null) return false;

        Node node = this.findNode(key, nl);
        if (node == null) return false;
        nl.remove(node);
        return true;
    }

    private Node findNode(String key, NodeList nodeList) {
        if (nodeList == null) return null;

        Iterator<Node> it = nodeList.iterator();
        while (it.hasNext()) {
            Node node = it.next();
            if (node.key.equals(key)) {
                return node;
            }
        }

        return null;
    }

    public boolean isRehashing() {
        if (this.rehashIdx >= this.ht1.length()) {
            this.ht1 = this.ht2;
            this.ht2 = null;
            this.rehashIdx = -1;
            return false;
        }

        if (this.rehashIdx >= 0) return true;

        final int capacity = this.ht1.length();
        if (this.ht1.size() >= (int) (capacity * this.loadFactor)) {
            this.ht2 = new KVArray(capacity * 2);
            this.rehashIdx = 0;
            return true;
        }

        return false;
    }

    public void rehash() {
        for (int i = 0; i < Math.min(this.ht1.length(), this.rehashBuckets); i++) {
            NodeList nodeList = this.ht1.get(this.rehashIdx);

            if (nodeList != null) {
                String key = nodeList.head.key;
                int bucketIndex = bucketIndex(key.hashCode(), this.ht2.length());
                NodeList nl = this.ht2.get(bucketIndex);
                if (nl != null) nl.merge(nodeList);
                else this.ht2.set(bucketIndex, nodeList);
            }

            this.rehashIdx += 1;
        }
    }

    /**
     * Returns a non-negative bucket index.
     */
    private int bucketIndex(int hashCode, int len) {
        return (hashCode & 0x7fffffff) % len;
    }


    public KVArray getHt1() {
        return this.ht1;
    }

    public KVArray getHt2() {
        return this.ht2;
    }

    public int capacity() {
        return isRehashing() ? this.ht2.length() : this.ht1.length();
    }

    public float loadFactor() {
        return this.loadFactor;
    }

    public int size() {
        return isRehashing() ? this.ht2.size() + this.ht1.size() : this.ht1.size();
    }

    public static class Node {
        private final String key;
        private byte[] value;
        private Node prev;
        private Node next;

        private Node(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return this.key;
        }

        public byte[] value() {
            return this.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.key);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof KVMap.Node other)) return false;
            return Objects.equals(this.key, other.key);
        }
    }

    public static class NodeList implements Iterable<Node> {
        private Node head;
        private Node tail;
        private int size;
        private final KVArray array;

        public NodeList(KVArray array) {
            this.array = array;
        }

        /**
         * Appends the node to the tail of the list
         *
         * @param node
         */
        public void add(Node node) {
            if (this.head == null) {
                this.head = node;
                this.tail = node;
            } else {
                this.tail.next = node;
                node.prev = this.tail;
                this.tail = node;
            }

            ++this.size;
            ++this.array.size;
        }

        /**
         * Prepends the nodes in other NodeList into this list
         * and increments size of this NodeList and the parent KVArray.
         *
         * @param other
         */
        public void merge(NodeList other) {
            other.tail.next = this.head;
            this.head.prev = other.tail;
            this.head = other.head;

            this.array.size += other.size;
            this.size += other.size;
        }

        public void remove(Node node) {
            if (node.prev != null) {
                node.prev.next = node.next;
            }

            if (node.next != null) {
                node.next.prev = node.prev;
            }

            if (node == this.head) {
                this.head = node.next;
            }

            if (node == this.tail) {
                this.tail = node.prev;
            }

            node.prev = null;
            node.next = null;

            --this.size;
            --this.array.size;
        }

        public int size() {
            return this.size;
        }

        public Iterator<Node> iterator() {
            return new NodeIterator(this.head, this);
        }
    }

    private static class NodeIterator implements Iterator<Node> {
        private Node cur;
        private Node next;
        private final NodeList nodeList;

        private NodeIterator(Node cur, NodeList nodeList) {
            this.next = cur;
            this.nodeList = nodeList;
        }

        @Override
        public boolean hasNext() {
            return this.next != null;
        }

        @Override
        public Node next() {
            this.cur = this.next;
            this.next = this.cur.next;
            return this.cur;
        }

        @Override
        public void remove() {
            this.nodeList.remove(this.cur);
        }
    }

    public class KVArray implements Iterable<NodeList> {
        public static final int INITIAL_CAPACITY = 1000;
        private final int capacity;
        private int size;
        private final NodeList[] arr;

        private KVArray() {
            this(KVArray.INITIAL_CAPACITY);
        }

        private KVArray(int initialCapacity) {
            this.capacity = initialCapacity;
            this.arr = new NodeList[this.capacity];
        }

        public NodeList get(int index) {
            if (index < 0 || index >= this.arr.length) {
                return null;
            }

            return this.arr[index];
        }

        public void set(int index, NodeList value) {
            if (index < 0 || index >= this.arr.length) {
                throw new ArrayIndexOutOfBoundsException("Index: " + index + ", Size: " + this.arr.length);
            }

            this.arr[index] = value;
        }

        @Override
        public Iterator<NodeList> iterator() {
            return new NodeListIterator();
        }

        public int size() {
            return this.size;
        }

        public int length() {
            return this.capacity;
        }

        private class NodeListIterator implements Iterator<NodeList> {
            private int index = 0;

            private NodeListIterator() {
            }

            @Override
            public boolean hasNext() {
                this.index = nextIndex(this.index);
                return this.index != -1;
            }

            @Override
            public NodeList next() {
                if (this.index == -1) {
                    throw new NoSuchElementException();
                }

                NodeList nodeList = KVArray.this.arr[this.index];
                this.index++;
                return nodeList;
            }

            /**
             * @param start Start index
             * @return Returns the next index with a non-null value.
             * Else -1 if it couldn't be found.
             */
            private int nextIndex(int start) {
                for (int i = start; i < KVArray.this.arr.length; i++) {
                    if (KVArray.this.arr[i] != null) return i;
                }

                return -1;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }
}