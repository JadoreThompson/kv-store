package main.java.com.zenz.kvstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.zenz.kvstore.KVMap;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;


public class MyTest {

    @Test
    public void serialisationTest() throws IOException, ClassNotFoundException {
        KVMap.Node[] arr = new KVMap.Node[100_000];

        for (int i = 0; i < arr.length; i++) {
            KVMap.Node node = null;
            for (int j = 0; j < 5; j++) {
                byte[] byteArr = new byte[]{1, 2, 3, 4, 5};
                KVMap.Node newNode = new KVMap.Node(i + ":" + j, byteArr);
                if (node != null) {
                    node.next = newNode;
                } else {
                    node = newNode;
                }
            }
        }

        Path tmpFpath = Files.createTempFile("test-", ".bin");


        Instant start = Instant.now();
        ObjectOutputStream fromParent = new ObjectOutputStream(new FileOutputStream(tmpFpath.toString()));
        fromParent.writeObject(arr);
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);

        System.out.println("Serialisation took " + duration.toSeconds() + "seconds");
        System.out.println("Serialisation took " + duration.toMillis() + "milliseconds");
        System.out.println("Serialisation took " + duration.toNanos() + "nanoseconds");

        // Deserialization
        Instant deserialStart = Instant.now();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tmpFpath.toString()))) {
            KVMap.Node[] deserializedArr = (KVMap.Node[]) ois.readObject();
        }
        Instant deserialEnd = Instant.now();
        Duration deserialDuration = Duration.between(deserialStart, deserialEnd);

        System.out.println("Deserialisation took " + deserialDuration.toSeconds() + " seconds");
        System.out.println("Deserialisation took " + deserialDuration.toMillis() + " milliseconds");
        System.out.println("Deserialisation took " + deserialDuration.toNanos() + " nanoseconds");
    }

    @Test
    public void kryoSerialisationTest() throws IOException {
        KVMap.Node[] arr = new KVMap.Node[100_000];

        for (int i = 0; i < arr.length; i++) {
            KVMap.Node node = null;
            for (int j = 0; j < 5; j++) {
                byte[] byteArr = new byte[]{1, 2, 3, 4, 5};
                KVMap.Node newNode = new KVMap.Node(i + ":" + j, byteArr);
                if (node != null) {
                    node.next = newNode;
                } else {
                    node = newNode;
                }
            }
        }

        Kryo kryo = new Kryo();
        kryo.register(KVMap.Node.class);
        kryo.register(KVMap.Node[].class);

        Path tmpFpath = Files.createTempFile("test-kyro-", ".bin");

        Instant start = Instant.now();
        try (Output output = new Output(new FileOutputStream(tmpFpath.toString()))) {
            kryo.writeObject(output, arr);
        }
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);

        System.out.println("Kryo Serialisation took " + duration.toSeconds() + " seconds");
        System.out.println("Kryo Serialisation took " + duration.toMillis() + " milliseconds");
        System.out.println("Kryo Serialisation took " + duration.toNanos() + " nanoseconds");

        // Deserialization
        Instant deserialStart = Instant.now();
        try (Input input = new Input(new FileInputStream(tmpFpath.toString()))) {
            KVMap.Node[] deserializedArr = kryo.readObject(input, KVMap.Node[].class);
        }
        Instant deserialEnd = Instant.now();
        Duration deserialDuration = Duration.between(deserialStart, deserialEnd);

        System.out.println("Kryo Deserialisation took " + deserialDuration.toSeconds() + " seconds");
        System.out.println("Kryo Deserialisation took " + deserialDuration.toMillis() + " milliseconds");
        System.out.println("Kryo Deserialisation took " + deserialDuration.toNanos() + " nanoseconds");
    }
}
