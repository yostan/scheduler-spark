package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/27.
 */
public class URIHandlerException extends XException {

    public URIHandlerException(XException cause) {
        super(cause);
    }

    public URIHandlerException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
