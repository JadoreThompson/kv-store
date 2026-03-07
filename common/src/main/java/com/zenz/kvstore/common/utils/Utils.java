package com.zenz.kvstore.common.utils;


import com.zenz.kvstore.common.CheckedRunnable;

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
