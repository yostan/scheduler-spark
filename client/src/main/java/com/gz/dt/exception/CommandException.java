package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/27.
 */
public class CommandException extends XException {

    public CommandException(XException cause) {
        super(cause);
    }

    public CommandException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }
}
