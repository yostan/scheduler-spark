package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/27.
 */
public class HadoopAccessorException extends URIHandlerException {

    public HadoopAccessorException(XException cause) {
        super(cause);
    }

    public HadoopAccessorException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
