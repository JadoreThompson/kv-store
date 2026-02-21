package com.zenz.kvstore;

import java.util.function.Consumer;

public class KVArray {
    private final KVMap.NodeList[] arr;
    private int size = 0;

    public KVArray() {
        arr = new KVMap.NodeList[1000];
    }

    public KVArray(int length) {
        arr = new KVMap.NodeList[length];
    }

    public void add(int index, KVMap.NodeList value) {
        if (arr[index] == null) {
            size++;
        }
        arr[index] = value;
    }

    public void setNull(int index) {
        if (arr[index] != null) {
            size--;
        }

        arr[index] = null;
    }

    public KVMap.NodeList get(int index) {
        return arr[index];
    }

    public void forEach(Consumer<KVMap.NodeList> consumer) {
        for (int i = 0; i < size; i++) {
            consumer.accept(arr[i]);
        }
    }

    public int size() {
        return size;
    }

    public int length() {
        return arr.length;
    }
}
