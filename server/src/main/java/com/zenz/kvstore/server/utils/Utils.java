package com.zenz.kvstore.server.utils;

import com.zenz.kvstore.server.CheckedRunnable;

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
