package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.XLog;

import javax.servlet.ServletException;

/**
 * Created by naonao on 2015/10/27.
 */
public class XServletException extends ServletException {
    private static final long serialVersionUID = 1L;
    private ErrorCode errorCode;
    private int httpStatusCode;

    public XServletException(int httpStatusCode, XException ex) {
        super(ex.getMessage(), ex);
        this.errorCode = ex.getErrorCode();
        this.httpStatusCode = httpStatusCode;
    }

    public XServletException(int httpStatusCode, ErrorCode errorCode, Object... params) {
        super(errorCode.format(params), XLog.getCause(params));
        this.errorCode = errorCode;
        this.httpStatusCode = httpStatusCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }
}
