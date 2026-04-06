package com.zenz.kvstore.client;

import com.zenz.kvstore.common.commands.*;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.enums.ResponseType;
import com.zenz.kvstore.common.responses.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * A simple client implementation to interact with a KVServer instance.
 */
public class KVStoreClient {
    public String host = "localhost";
    public int port = 6767;
    public final int MAX_RETRY_ATTEMPTS = 5;
    public final int RETRY_TIMEOUT_MS = 5000;

    private Socket socket;
    private BufferedInputStream in;
    private BufferedOutputStream out;
    private boolean running;

    private Command prevCommand;
    private Class<? extends BaseResponse> prevResponseClass;
    private int curRetryAttempts;

    public KVStoreClient(String host, int port) {
        this.host = host;
        this.port = port;
        running = false;
    }

    public KVStoreClient() {
    }

    public void connect() throws IOException {
        if (running) return;

        socket = new Socket(host, port);
        out = new BufferedOutputStream(socket.getOutputStream());
        in = new BufferedInputStream(socket.getInputStream());
        running = true;
    }

    public void disconnect() throws IOException {
        if (!running) return;
        running = false;
        socket.close();
        in.close();
        out.close();
    }

    public GetResponse get(String key) throws IOException, InterruptedException {
        prevCommand = new GetCommand(key);
        prevResponseClass = GetResponse.class;
        return (GetResponse) send(ByteBuffer.wrap(prevCommand.serialize()), prevResponseClass);
    }

    public PutResponse put(String key, byte[] value) throws IOException, InterruptedException {
        prevCommand = new PutCommand(key, value);
        prevResponseClass = PutResponse.class;
        return (PutResponse) send(ByteBuffer.wrap(prevCommand.serialize()), prevResponseClass);
    }

    public DeleteResponse delete(String key) throws IOException, InterruptedException {
        prevCommand = new DeleteCommand(key);
        prevResponseClass = DeleteResponse.class;
        return (DeleteResponse) send(ByteBuffer.wrap(prevCommand.serialize()), prevResponseClass);
    }

    public SearchResponse search(String prefix) throws IOException, InterruptedException {
        prevCommand = new SearchCommand(prefix);
        prevResponseClass = SearchResponse.class;
        return (SearchResponse) send(ByteBuffer.wrap(prevCommand.serialize()), prevResponseClass);
    }

    private BaseResponse send(
            ByteBuffer buffer,
            Class<? extends BaseResponse> clazz
    ) throws IOException, InterruptedException {
        // Sending message
        while (buffer.hasRemaining()) {
            out.write(buffer.get());
        }
        out.flush();

        ByteBuffer result = ByteBuffer.allocate(1024);
        in.read(result.array());

        // Handling response
        int typeValue = result.getInt();
        ResponseType type = ResponseType.fromValue(typeValue);
        result.rewind();
        BaseResponse response = null;

        if (type.equals(ResponseType.ERROR_RESPONSE)) {
            // Handling error
            BaseErrorResponse errorResponse = ErrorResponse.deserialize(result);
            ErrorType errorType = errorResponse.errorType();
            String errorMessage = null;

            if (errorType.equals(ErrorType.NOT_CONTROLLER)) {
                errorMessage = "Node is no longer controller";
                response = handleRedirectResponse((RedirectResponse) errorResponse);
            } else if (errorType.equals(ErrorType.IN_ELECTION)) {
                response = handleRetry();
            } else {
                errorMessage = ((ErrorResponse) errorResponse).message();
            }

            if (response == null) {
                throw new KVStoreClientException(errorType + errorMessage);
            }
        } else {
            response = BaseResponse.deserialize(result);
        }

        if (!(response.getClass().equals(clazz))) {
            throw new KVStoreClientException("Invalid response type. Expected " + clazz.getName() + " received " + response);
        }

        return response;
    }

    private BaseResponse handleRedirectResponse(RedirectResponse response) throws IOException, InterruptedException {
        InetSocketAddress address = response.address();
        host = address.getHostName();
        port = address.getPort();
        connect();
        return send(ByteBuffer.wrap(prevCommand.serialize()), prevResponseClass);
    }

    private BaseResponse handleRetry() throws InterruptedException, IOException {
        if (curRetryAttempts >= MAX_RETRY_ATTEMPTS) {
            curRetryAttempts = 0;
            throw new KVStoreClientException("Maximum retry attempts reached");
        }

        curRetryAttempts++;
        Thread.sleep(RETRY_TIMEOUT_MS);
        return send(ByteBuffer.wrap(prevCommand.serialize()), prevResponseClass);
    }
}