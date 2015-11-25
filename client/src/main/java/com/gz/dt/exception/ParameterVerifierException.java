package com.gz.dt.exception;

import com.gz.dt.util.ErrorCode;

/**
 * Created by naonao on 2015/10/28.
 */
public class ParameterVerifierException extends XException {

    public ParameterVerifierException(XException cause) {
        super(cause);
    }


    public ParameterVerifierException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }

}
