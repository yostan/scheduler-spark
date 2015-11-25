package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.ParamChecker;
import com.gz.dt.util.XLog;

/**
 * Created by naonao on 2015/10/26.
 */
public class XException extends Exception {
    private ErrorCode errorCode;

    private XException(String message, ErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = ParamChecker.notNull(errorCode, "errorCode");
    }

    public XException(XException cause) {
        this(cause.getMessage(), cause.getErrorCode(), cause);
    }

    public XException(ErrorCode errorCode, Object... params) {
        this(errorCode.format(params), errorCode, XLog.getCause(params));
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

}
