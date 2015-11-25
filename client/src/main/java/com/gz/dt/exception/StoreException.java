package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/28.
 */
public class StoreException extends XException {

    public StoreException(XException cause) {
        super(cause);
    }

    public StoreException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
