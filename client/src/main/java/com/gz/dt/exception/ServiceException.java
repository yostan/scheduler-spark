package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/26.
 */
public class ServiceException extends XException {

    public ServiceException(XException cause) {
        super(cause);
    }

    public ServiceException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
