package com.zenz.kvstore.client;

import com.zenz.kvstore.common.commands.GetCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.enums.ResponseType;
import com.zenz.kvstore.common.responses.ErrorResponse;
import com.zenz.kvstore.common.responses.GetResponse;
import com.zenz.kvstore.common.responses.PutResponse;

import java.io.*;

import java.net.Socket;
import java.nio.ByteBuffer;

public class KVStoreClient {

    /**
     * Default host address for the KV-store server
     */
    private static final String DEFAULT_HOST = "localhost";

    /**
     * Default port for the KV-store server
     */
    private static final int DEFAULT_PORT = 6767;

    /**
     * The host address of the KV-store server
     */
    public final String host;

    /**
     * The port number of the KV-store server
     */
    public final int port;

    private Socket socket;
    private BufferedInputStream in;
    private BufferedOutputStream out;

    private boolean running;

    /**
     * Creates a new KVStoreClient with the specified host and port.
     *
     * @param host the hostname or IP address of the KV-store server
     * @param port the port number of the KV-store server
     */
    public KVStoreClient(String host, int port) {
        this.host = host;
        this.port = port;
        running = false;
    }

    /**
     * Creates a new KVStoreClient with default settings.
     *
     * <p>Uses localhost:6767 as the default server address.</p>
     */
    public KVStoreClient() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    /**
     * Establishes a connection to the KV-store server.
     *
     * <p>If the client is already connected, this method returns immediately
     * without performing any action.</p>
     *
     * @throws IOException if an I/O error occurs when creating the socket connection
     */
    public void connect() throws IOException {
        if (running) return;

        socket = new Socket(host, port);
        out = new BufferedOutputStream(socket.getOutputStream());
        in = new BufferedInputStream(socket.getInputStream());
        running = true;
    }

    /**
     * Disconnects from the KV-store server.
     *
     * <p>If the client is not currently connected, this method returns immediately
     * without performing any action.</p>
     *
     * @throws IOException if an I/O error occurs when closing the connection
     */
    public void disconnect() throws IOException {
        if (!running) return;
        running = false;
        socket.close();
        in.close();
        out.close();
    }

    /**
     * Retrieves the value associated with the specified key from the KV-store.
     *
     * <p>Sends a GET request to the server and returns the value as a ByteBuffer.
     * If the key does not exist or an error occurs, a {@link KVStoreClientException} is thrown.</p>
     *
     * @param key the key to retrieve
     * @return a ByteBuffer containing the value associated with the key
     * @throws IOException            if an I/O error occurs during communication with the server
     * @throws KVStoreClientException if the server returns an error response
     */
    public GetResponse get(String key) throws IOException {
        ByteBuffer result = send(ByteBuffer.wrap(new GetCommand(key).serialize()));
        checkIfError(result);

        return GetResponse.deserialize(result);
    }

    /**
     * Stores a key-value pair in the KV-store.
     *
     * <p>Sends a PUT request to the server to store the specified value under the given key.
     * If the key already exists, its value will be overwritten.</p>
     *
     * @param key   the key under which to store the value
     * @param value the value to store as a byte array
     * @return {@code true} if the operation was successful
     * @throws IOException            if an I/O error occurs during communication with the server
     * @throws KVStoreClientException if the server returns an error response
     */
    public PutResponse put(String key, byte[] value) throws IOException {
        ByteBuffer result = send(ByteBuffer.wrap(new PutCommand(key, value).serialize()));
        return PutResponse.deserialize(result);
    }

    private ByteBuffer send(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            out.write(buffer.get());
        }
        out.flush();

        ByteBuffer inBuffer = ByteBuffer.allocate(1024);
        in.read(inBuffer.array());

        int typeValue = inBuffer.getInt();
        ResponseType type = ResponseType.fromValue(typeValue);
        inBuffer.rewind();

        if (type.equals(ResponseType.ERROR_RESPONSE)) {
            ErrorResponse response = ErrorResponse.deserialize(inBuffer);
            throw new KVStoreClientException(
                    response.errorType() +
                            ((response.message() != null) ? " " + response.message() : "")
            );
        }

        return inBuffer;
    }


    /**
     * Removes the "OK " prefix from the server response.
     *
     * @param buffer the ByteBuffer containing the server response
     * @return a ByteBuffer containing the response data without the "OK " prefix
     */
    private ByteBuffer parseResult(ByteBuffer buffer) {
        return buffer.slice(3, buffer.capacity() - 3);
    }

    /**
     * Checks if the server response indicates an error.
     *
     * <p>If the response starts with "OK", this method returns normally.
     * Otherwise, it extracts the error message and throws a {@link KVStoreClientException}.</p>
     *
     * @param buffer the ByteBuffer containing the server response
     * @throws KVStoreClientException if the server returned an error response
     */
    private void checkIfError(ByteBuffer buffer) {
        byte[] arr = buffer.array();
        String prefix = "" + (char) arr[0] + (char) arr[1];
        if (prefix.equals("OK")) return;

        StringBuilder builder = new StringBuilder();
        buffer.position(6);

        byte b;
        while ((b = buffer.get()) != 0) {
            builder.append((char) b);
        }

        throw new KVStoreClientException(builder.toString());
    }


    /**
     * Exception thrown when the KV-store client encounters an error.
     *
     * <p>This is a runtime exception used to indicate client-side errors
     * that are not related to I/O operations.</p>
     */
    public static class KVStoreClientException extends RuntimeException {

        /**
         * Constructs a new KVStoreClientException with the specified message.
         *
         * @param message the detail message
         */
        public KVStoreClientException(String message) {
            super(message);
        }

    }
}