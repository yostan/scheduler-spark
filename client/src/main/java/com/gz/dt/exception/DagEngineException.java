package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/27.
 */
public class DagEngineException extends BaseEngineException {

    public DagEngineException(XException cause) {
        super(cause);
    }

    public DagEngineException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
