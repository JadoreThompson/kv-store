package com.zenz.kvstore.server.command.handler;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.server.KVStore;
import com.zenz.kvstore.server.raft.Manager;

import java.nio.ByteBuffer;

public class RaftCommandHandler implements BaseCommandHandler {
    private final KVStore store;
    private final Manager manager;

    public RaftCommandHandler(KVStore store, Manager manager) {
        this.store = store;
        this.manager = manager;
    }

    //    public ByteBuffer handleCommand(Command command) {
//        try {
//            NodeRole nodeState = this.manager.getRole();
//
//            if (nodeState.equals(NodeRole.CONTROLLER)) {
//                Server server = this.manager.getServer();
//
//                CommandType commandType = command.type();
//                if (commandType.equals(CommandType.GET)) {
//                    GetCommand comm = (GetCommand) command;
//                    KVMap.Node node = store.get(comm.key());
//                    return ByteBuffer.wrap(new GetResponse(node == null ? null : node.value()).serialize());
//                }
//
//                CompletableFuture<Boolean> fut = new CompletableFuture<>();
//                server.handleCommand(command, fut);
//                fut.get();
//
//                if (commandType.equals(CommandType.PUT)) {
//                    PutCommand comm = (PutCommand) command;
//                    store.put(comm.key(), comm.value());
//                    return ByteBuffer.wrap(new PutResponse().serialize());
//                }
//
//                if (commandType.equals(CommandType.DELETE)) {
//                    DeleteCommand comm = (DeleteCommand) command;
//                    boolean success = store.delete(comm.key());
//                    return ByteBuffer.wrap(new DeleteResponse(success).serialize());
//                }
//
//                return ByteBuffer.wrap(new ErrorResponse(ErrorType.UNSUPPORTED_OPERATION, null).serialize());
//            }
//
//            if (nodeState.equals(NodeRole.CANDIDATE)) {
//                return ByteBuffer.wrap(new ErrorResponse(ErrorType.IN_ELECTION, null).serialize());
//            }
//
//            NodeConfig controllerConfig = this.manager.getControllerConfig();
//            if (controllerConfig == null) {
//                return ByteBuffer.wrap(new ErrorResponse(ErrorType.CONTROLLER_UNKNOWN, null).serialize());
//            }
//
//            return ByteBuffer.wrap(new RedirectResponse(controllerConfig.clientAddress()).serialize());
//        } catch (IOException e) {
//            e.printStackTrace();
//            return ByteBuffer.wrap(
//                    new ErrorResponse(ErrorType.SERVER_ERROR, "IO Exception: " + e.getMessage()).serialize()
//            );
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            return ByteBuffer.wrap(
//                    new ErrorResponse(ErrorType.SERVER_ERROR, "Thread was interrupted: " + e.getMessage()).serialize()
//            );
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//            return ByteBuffer.wrap(
//                    new ErrorResponse(ErrorType.SERVER_ERROR, "Exception awaiting controller: " + e.getMessage()).serialize()
//            );
//        }
//    }

    @Override
    public ByteBuffer handleCommand(final Command command) {
        return null;
    }
}
