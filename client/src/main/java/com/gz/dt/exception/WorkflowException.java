package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/27.
 */
public class WorkflowException extends XException {

    public WorkflowException(XException cause) {
        super(cause);
    }

    public WorkflowException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
