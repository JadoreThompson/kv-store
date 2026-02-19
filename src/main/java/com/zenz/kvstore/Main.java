package com.zenz.kvstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws IOException {
//        final String FNAME = "test.log";
//        WALogger logger = new WALogger(FNAME);
//        logger.close();

        byte[] data = {0x48, 0x65, 0x6C, 0x6C, 0x6F};
        Files.write(Path.of("tmp.txt"), data);
    }
}