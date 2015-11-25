package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/27.
 */
public class BaseEngineException extends XException {

    public BaseEngineException(XException cause) {
        super(cause);
    }

    public BaseEngineException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
