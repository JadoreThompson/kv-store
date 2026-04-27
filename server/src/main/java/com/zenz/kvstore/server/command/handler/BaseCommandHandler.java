package com.zenz.kvstore.server.command.handler;

import com.zenz.kvstore.common.command.Command;
import com.zenz.kvstore.common.response.BaseResponse;

public interface BaseCommandHandler {

    BaseResponse handleCommand(Command command);
}
