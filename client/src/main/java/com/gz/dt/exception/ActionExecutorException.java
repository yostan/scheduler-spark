package com.gz.dt.exception;

import com.gz.dt.util.ParamChecker;
import com.gz.dt.util.XLog;

/**
 * Created by naonao on 2015/10/28.
 */
public class ActionExecutorException extends Exception {

    public static enum ErrorType {

        /**
         * The action will be automatically retried by Oozie.
         */
        TRANSIENT,

        /**
         * The job in set in SUSPEND mode and it will wait for an admin to resume the job.
         */

        NON_TRANSIENT,

        /**
         * The action completes with an error transition.
         */
        ERROR,

        /**
         * The action fails. No transition is taken.
         */
        FAILED
    }


    private ErrorType errorType;
    private String errorCode;


    public ActionExecutorException(ErrorType errorType, String errorCode, String message) {
        super(message);
        this.errorType = ParamChecker.notNull(errorType, "errorType");
        this.errorCode = ParamChecker.notEmpty(errorCode, "errorCode");
    }

    public ActionExecutorException(ErrorType errorType, String errorCode, String messageTemplate, Object... params) {
        super(errorCode + ": " + XLog.format(messageTemplate, params), XLog.getCause(params));
        this.errorType = ParamChecker.notNull(errorType, "errorType");
        this.errorCode = ParamChecker.notEmpty(errorCode, "errorCode");
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public String getErrorCode() {
        return errorCode;
    }

}
