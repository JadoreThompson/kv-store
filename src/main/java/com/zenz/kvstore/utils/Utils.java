package com.zenz.kvstore.utils;

import com.zenz.kvstore.CheckedRunnable;

import java.util.Objects;

public class Utils {
    public static void runnableWrapper(CheckedRunnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            System.err.println("Exception in Thread '" + Thread.currentThread().getName() + "' " + e.getMessage());
            e.printStackTrace();
        }
    }
}
