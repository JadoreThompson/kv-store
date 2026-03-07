package com.zenz.kvstore.common.responses;

import com.zenz.kvstore.common.enums.ErrorType;


public interface BaseErrorResponse extends BaseResponse {
    ErrorType errorType();
}
