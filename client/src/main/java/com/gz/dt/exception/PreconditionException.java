package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/27.
 */
public class PreconditionException extends XException {

    public PreconditionException(XException cause) {
        super(cause);
    }

    public PreconditionException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
