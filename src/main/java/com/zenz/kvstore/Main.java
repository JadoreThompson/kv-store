package com.zenz.kvstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Main {
    public static void main(String[] args) throws IOException {
        final String FNAME = "test.log";
        WALogger logger = new WALogger(FNAME);
        logger.close();
    }
}