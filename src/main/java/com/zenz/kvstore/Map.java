package main.java.com.zenz.kvstore;

import java.util.Objects;

public class Map {
    public static int INITIAL_CAPACITY = 1000;
    public static float LOAD_FACTOR = 1.0f;
    private int capacity;
    private Array<NodeList> ht0;
    private Array<NodeList> ht1;
    private boolean isResizing;

    public Map() {
        capacity = INITIAL_CAPACITY;
        ht0 = new Array<>(capacity);
        isResizing = false;
    }

    public Node get(String key) {
        if (checkIsResizing()) {
            return getIsResizing(key);
        }

        int hashCode = key.hashCode();
        int ht0Index = bucketIndex(hashCode, capacity);
        NodeList nl = ht0.get(ht0Index);
        if (nl == null) {
            return null;
        }

        Node cur = nl.head;
        while (cur != null) {
            if (cur.key.equals(key)) {
                return cur;
            }
            cur = cur.next;
        }

        return null;
    }

    public void put(String key, byte[] value) {
        if (checkIsResizing()) {
            insertResizing(key, value);
            return;
        }

        int hashCode = key.hashCode();
        int ht0Index = bucketIndex(hashCode, capacity);
        NodeList nl = ht0.get(ht0Index);

        if (nl == null) {
            ht0.add(ht0Index, new NodeList(new Node(key, value)));
            return;
        }

        Node cur = nl.head;
        if (cur.key.equals(key)) {
            cur.value = value;
            return;
        }

        while (cur.next != null) {
            if (cur.next.key.equals(key)) {
                cur.next.value = value;
                return;
            }
            cur = cur.next;
        }

        cur.next = new Node(key, value);
    }

    private void insertResizing(String key, byte[] value) {
        Node node = null;
        int hashCode = key.hashCode();

        // Remove from ht0
        int ht0Index = bucketIndex(hashCode, capacity / 2);
        NodeList nl = ht0.get(ht0Index);

        if (nl != null) {
            if (nl.head.key.equals(key)) {
                node = nl.head;
                if (node.next == null) {
                    ht0.setNull(ht0Index);
                } else {
                    nl.head = node.next;
                    node.next = null;
                }
            } else {
                Node cur = nl.head;
                while (cur.next != null) {
                    if (cur.next.key.equals(key)) {
                        node = cur.next;
                        cur.next = node.next;
                        node.next = null;
                        break;
                    }
                    cur = cur.next;
                }
            }
        }

        // Add to ht1
        if (ht1 == null) {
            capacity *= 2;
            ht1 = new Array<>(capacity);
        }

        int ht1Index = bucketIndex(hashCode, capacity);
        nl = ht1.get(ht1Index);
        if (nl == null) {
            ht1.add(ht1Index, new NodeList(new Node(key, value)));
        } else {
            Node cur = nl.head;
            if (cur.key.equals(key)) {
                cur.value = value;
            } else {
                boolean found = false;
                while (cur.next != null) {
                    if (cur.next.key.equals(key)) {
                        cur.next.value = value;
                        found = true;
                        break;
                    }
                    cur = cur.next;
                }
                if (!found) {
                    cur.next = new Node(key, value);
                }
            }
        }

        if (ht0.size() == 0) {
            ht0 = ht1;
            ht1 = null;
            isResizing = false;
        }
    }

    public Node getIsResizing(String key) {
        int hashCode = key.hashCode();

        // Search and remove from ht0
        int ht0Index = bucketIndex(hashCode, capacity / 2);
        NodeList nl = ht0.get(ht0Index);
        if (nl == null) {
            // Not in ht0, search ht1
            if (ht1 != null) {
                int ht1Index = bucketIndex(hashCode, capacity);
                nl = ht1.get(ht1Index);
                if (nl == null) return null;
                Node cur = nl.head;
                while (cur != null) {
                    if (cur.key.equals(key)) return cur;
                    cur = cur.next;
                }
            }
            return null;
        }

        Node node = null;
        Node cur = nl.head;
        if (cur.key.equals(key)) {
            node = cur;
            if (cur.next == null) {
                ht0.setNull(ht0Index);
            } else {
                nl.head = cur.next;
                node.next = null;
            }
        } else {
            while (cur.next != null) {
                if (cur.next.key.equals(key)) {
                    node = cur.next;
                    cur.next = node.next;
                    node.next = null;
                    break;
                }
                cur = cur.next;
            }
        }

        if (node != null && ht1 != null) {
            int ht1Index = bucketIndex(hashCode, capacity);
            nl = ht1.get(ht1Index);
            if (nl == null) {
                ht1.add(ht1Index, new NodeList(node));
            } else {
                cur = nl.head;
                while (cur.next != null) {
                    cur = cur.next;
                }
                cur.next = node;
            }
        }

        return node;
    }

    private boolean checkIsResizing() {
        if (ht0.size() >= (int) (capacity * LOAD_FACTOR)) {
            isResizing = true;
        } else {
            isResizing = false;
        }
        return isResizing;
    }

    /**
     * Returns a non-negative bucket index.
     * Note: Forcing the sign bit to 0.
     */
    private int bucketIndex(int hashCode, int len) {
        return (hashCode & 0x7fffffff) % len;
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
