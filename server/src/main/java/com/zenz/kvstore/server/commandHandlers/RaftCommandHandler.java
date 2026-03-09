package com.zenz.kvstore.server.commandHandlers;

import com.zenz.kvstore.common.enums.CommandType;
import com.zenz.kvstore.common.enums.ErrorType;
import com.zenz.kvstore.common.commands.Command;
import com.zenz.kvstore.common.commands.GetCommand;
import com.zenz.kvstore.common.commands.PutCommand;
import com.zenz.kvstore.common.responses.ErrorResponse;
import com.zenz.kvstore.common.responses.GetResponse;
import com.zenz.kvstore.common.responses.PutResponse;
import com.zenz.kvstore.common.responses.RedirectResponse;
import com.zenz.kvstore.server.KVMap;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.raft.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RaftCommandHandler implements BaseCommandHandler {
    private final KVStore store;
    private final RaftManager manager;

    public RaftCommandHandler(KVStore store, RaftManager manager) {
        this.store = store;
        this.manager = manager;
    }

    public ByteBuffer handleCommand(SocketChannel channel, Command command) {
        try {
            NodeState nodeState = manager.getState();

            if (nodeState.equals(NodeState.CONTROLLER)) {
                RaftControllerServerHandler controller = manager.getControllerServerHandler();

                CommandType commandType = command.type();
                if (commandType.equals(CommandType.GET)) {
                    GetCommand comm = (GetCommand) command;
                    KVMap.Node node = store.get(comm.key());
//                    ByteBuffer buffer = ByteBuffer.allocate(3 + node.value.length);
//                    buffer.put("OK ".getBytes(StandardCharsets.UTF_8));
//                    buffer.put(node.value);
//                    buffer.flip();
//                    return buffer;
                    return ByteBuffer.wrap(new GetResponse(node.value).serialize());
                }

                CompletableFuture<Boolean> fut = new CompletableFuture<Boolean>();
                controller.handleCommand(command, fut);
                fut.get();

                if (commandType.equals(CommandType.PUT)) {
                    PutCommand comm = (PutCommand) command;
                    store.put(comm.key(), comm.value());
//                    return ByteBuffer.wrap("OK".getBytes(StandardCharsets.UTF_8));
                    return ByteBuffer.wrap(new PutResponse().serialize());
                }

//                return ByteBuffer.wrap("ERROR Unsupported command".getBytes(StandardCharsets.UTF_8));
                return ByteBuffer.wrap(new ErrorResponse(ErrorType.UNSUPPORTED_OPERATION, null).serialize());
            }

            if (nodeState.equals(NodeState.CANDIDATE)) {
//                return ByteBuffer.wrap("ERROR In election".getBytes(StandardCharsets.UTF_8));
                return ByteBuffer.wrap(new ErrorResponse(ErrorType.IN_ELECTION, null).serialize());
            }

//            RaftControllerClient controllerClient = manager.getControllerClient();
//            InetSocketAddress controllerAddress = controllerClient.getControllerAddress();

            RaftNode controllerNode = manager.getControllerNode();
            InetSocketAddress controllerAddress = controllerNode.serverAddress();

            // Ensure this command is from the controller. If it isn't then
            // we redirect the client to the controller.
            return ByteBuffer.wrap(new RedirectResponse(controllerAddress).serialize());
        } catch (IOException e) {
            e.printStackTrace();
            return ByteBuffer.wrap(
                    new ErrorResponse(ErrorType.SERVER_ERROR, "IO Exception: " + e.getMessage()).serialize()
            );
        } catch (InterruptedException e) {
            e.printStackTrace();
            return ByteBuffer.wrap(
                    new ErrorResponse(ErrorType.SERVER_ERROR, "Thread was interrupted: " + e.getMessage()).serialize()
            );
        } catch (ExecutionException e) {
            e.printStackTrace();
            return ByteBuffer.wrap(
                    new ErrorResponse(ErrorType.SERVER_ERROR, "Exception awaiting controller: " + e.getMessage()).serialize()
            );
        }
    }
}
