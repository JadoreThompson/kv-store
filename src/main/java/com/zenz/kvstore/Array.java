package com.zenz.kvstore;

import java.util.function.Consumer;

public class Array<T> {
    private final T[] arr;
    private int size = 0;

    public Array(int length) {
        arr = (T[]) new Object[length];
    }

    public void add(int index, T value) {
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

    public T get(int index) {
        return arr[index];
    }

    public void forEach(Consumer<T> consumer) {
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
