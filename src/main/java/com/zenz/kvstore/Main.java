package com.zenz.kvstore;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class Main {
    public static void main(String[] args) throws IOException {
        final String FNAME = "test.txt";
        WALogger logger = new WALogger(FNAME);

        logger.log("Hello world");
        logger.log("another one");

        logger.close();
    }
}