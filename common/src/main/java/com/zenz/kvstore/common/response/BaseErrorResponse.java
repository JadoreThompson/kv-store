package com.zenz.kvstore.common.response;

import com.zenz.kvstore.common.enums.ErrorType;


public interface BaseErrorResponse extends BaseResponse {
    ErrorType errorType();
}
