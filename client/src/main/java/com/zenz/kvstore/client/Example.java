package com.zenz.kvstore.client;

import com.zenz.kvstore.common.response.GetResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class Example {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9877;
        final KVStoreClient client = new KVStoreClient("localhost", port);

        try {
            client.connect();

            final String key = "testkey";
            final String value = generateRandomString(20);

            System.out.println("Performing PUT key=" + key + " value=" + value);
            client.put(key, value.getBytes(StandardCharsets.UTF_8));
            System.out.println("Performing GET key=" + key);
            final GetResponse response = client.get(key);
            System.out.println("Response value " + new String(response.value(), StandardCharsets.UTF_8));
        } finally {
            client.disconnect();
        }
    }

    private static String generateRandomString(final int length) {
        final Random random = new Random();
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < length; i++) {
            builder.append((char) random.nextInt(65, 123));
        }

        return builder.toString();
    }
}
