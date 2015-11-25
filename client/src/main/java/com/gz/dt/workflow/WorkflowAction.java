package com.gz.dt.workflow;

import java.util.Date;

/**
 * Created by naonao on 2015/10/28.
 */
public interface WorkflowAction {

    String getId();

    String getName();

    String getCred();

    String getType();

    String getConf();

    Status getStatus();

    int getRetries();

    int getUserRetryCount();

    int getUserRetryMax();

    int getUserRetryInterval();

    Date getStartTime();

    Date getEndTime();

    String getTransition();

    String getData();

    String getStats();

    String getExternalChildIDs();

    String getExternalId();

    String getExternalStatus();

    String getTrackerUri();

    String getConsoleUrl();

    String getErrorCode();

    String getErrorMessage();






    public static enum Status {
        PREP,
        RUNNING,
        OK,
        ERROR,
        USER_RETRY,
        START_RETRY,
        START_MANUAL,
        DONE,
        END_RETRY,
        END_MANUAL,
        KILLED,
        FAILED, }
}
